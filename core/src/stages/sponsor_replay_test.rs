//! Pipeline test: a sponsor cannot replay one victim authorization by varying
//! its own first signature.
//!
//! An untrusted sponsor is the fee payer (first signer) of a two-signer SPL
//! Token transfer that a victim owner authorized as the second signer. ed25519
//! verification does not require unique signatures, so the sponsor can vary its
//! nonce and produce a second valid first signature over the byte-identical
//! message while reusing the victim's signature. Both variants pass sigverify;
//! keying dedup on the message hash collapses them to one so the transfer
//! executes only once.
//!
//! The test wires the real `ingress -> sigverify -> dedup` pipeline exactly as
//! `node.rs` does. Keyed on the first signature both variants would forward,
//! letting the sponsor replay the authorization; keyed on the message hash the
//! replay is dropped.

use {
    crate::{
        health::StageHeartbeat,
        stage_metrics::NoopMetrics,
        stages::{
            create_ingress_channel, start_dedup, start_sigverify_workerpool, DedupArgs,
            SigverifyArgs,
        },
    },
    ed25519_dalek::{
        hazmat::{raw_sign, ExpandedSecretKey},
        Signature as DalekSignature, VerifyingKey,
    },
    sha2::Sha512,
    solana_sdk::{
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        transaction::{SanitizedTransaction, Transaction},
    },
    std::{
        collections::{HashMap, HashSet, LinkedList},
        sync::Arc,
        time::Duration,
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

/// Forge a second valid ed25519 signature for `message_data` under `payer`.
///
/// Rebuilds the signer from its 32-byte seed, then flips a byte of the
/// nonce-deriving `hash_prefix`. The secret scalar (and thus the public key) is
/// unchanged, so `raw_sign` yields a different but still valid signature: the
/// nonce-variation replay, without touching the victim's part.
fn varied_nonce_signature(payer: &Keypair, message_data: &[u8]) -> Signature {
    let keypair_bytes = payer.to_bytes();
    let seed: [u8; 32] = keypair_bytes[..32]
        .try_into()
        .expect("32-byte ed25519 seed");

    let mut esk = ExpandedSecretKey::from(&seed);
    let verifying_key = VerifyingKey::from(&esk);
    assert_eq!(
        verifying_key.to_bytes(),
        payer.pubkey().to_bytes(),
        "derived verifying key must match the payer pubkey"
    );

    esk.hash_prefix[0] ^= 0xa5;
    let sig: DalekSignature = raw_sign::<Sha512>(&esk, message_data, &verifying_key);
    Signature::from(sig.to_bytes())
}

#[tokio::test(flavor = "multi_thread")]
async fn sponsor_cannot_replay_spl_transfer_under_varied_first_signature() {
    // Wire ingress -> sigverify -> dedup like the write pipeline in node.rs.
    let (ingress_tx, ingress_rx) = create_ingress_channel(64);
    let (dedup_tx, dedup_rx) = mpsc::channel::<SanitizedTransaction>(64);
    let (output_tx, mut output_rx) = mpsc::channel::<SanitizedTransaction>(64);
    let (blockhash_tx, blockhash_rx) = mpsc::unbounded_channel::<Hash>();
    let shutdown = CancellationToken::new();

    let _sigverify_workers = start_sigverify_workerpool(SigverifyArgs {
        num_workers: 2,
        admin_keys: vec![],
        rx: ingress_rx,
        output_tx: dedup_tx,
        metrics: Arc::new(NoopMetrics),
        heartbeat: StageHeartbeat::new(),
    })
    .await;

    let (_dedup, _live_blockhashes) = start_dedup(DedupArgs {
        max_blockhashes: 8,
        input_rx: dedup_rx,
        settled_blockhashes_rx: blockhash_rx,
        output_tx,
        initial_live_blockhashes: LinkedList::new(),
        initial_dedup_cache: HashMap::new(),
        metrics: Arc::new(NoopMetrics),
        heartbeat: StageHeartbeat::new(),
    })
    .await;

    // Make the transfer's blockhash live so dedup does not reject it as unknown.
    let blockhash = Hash::new_unique();
    blockhash_tx.send(blockhash).expect("seed live blockhash");
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Sponsor pays fees (first signer); victim owner authorizes the transfer
    // (second signer). This is the classic two-signer SPL Token transfer.
    let sponsor = Keypair::new();
    let victim = Keypair::new();
    let from_ata = Pubkey::new_unique();
    let to_ata = Pubkey::new_unique();
    let transfer_ix = spl_token::instruction::transfer(
        &spl_token::id(),
        &from_ata,
        &to_ata,
        &victim.pubkey(),
        &[],
        1_000,
    )
    .expect("build spl transfer");

    let tx_a = Transaction::new_signed_with_payer(
        &[transfer_ix],
        Some(&sponsor.pubkey()),
        &[&sponsor, &victim],
        blockhash,
    );

    // Variant B keeps the message and the victim's signature; only the sponsor's
    // first signature is swapped for another valid one.
    let message_data = tx_a.message_data();
    let mut tx_b = tx_a.clone();
    tx_b.signatures[0] = varied_nonce_signature(&sponsor, &message_data);

    let san_a = SanitizedTransaction::try_from_legacy_transaction(tx_a, &HashSet::new())
        .expect("sanitize a");
    let san_b = SanitizedTransaction::try_from_legacy_transaction(tx_b, &HashSet::new())
        .expect("sanitize b");

    // The premise: same authorization, distinct first signatures, and both
    // variants pass real signature verification.
    assert_eq!(
        san_a.message_hash(),
        san_b.message_hash(),
        "variants must share the signed message"
    );
    assert_ne!(
        san_a.signature(),
        san_b.signature(),
        "variants must carry distinct first signatures"
    );
    assert!(san_a.verify().is_ok(), "variant A must pass sigverify");
    assert!(
        san_b.verify().is_ok(),
        "variant B must also pass sigverify (both signatures are valid)"
    );

    // First variant flows all the way through and is cached before we send B.
    ingress_tx.send(san_a).await.expect("send variant A");
    let first = tokio::time::timeout(Duration::from_millis(500), output_rx.recv()).await;
    assert!(
        matches!(first, Ok(Some(_))),
        "first authorization must be forwarded, got {first:?}"
    );

    // The replay shares the message hash, so dedup drops it after sigverify.
    ingress_tx.send(san_b).await.expect("send variant B");
    let second = tokio::time::timeout(Duration::from_millis(300), output_rx.recv()).await;
    assert!(
        second.is_err(),
        "replayed variant must be deduped, not forwarded ({second:?})"
    );

    shutdown.cancel();
}
