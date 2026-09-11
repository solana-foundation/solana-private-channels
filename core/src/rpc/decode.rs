//! Turning submitted bytes into a transaction, for every version the channel accepts.
//!
//! Both write endpoints decode the same way, so the size rule and the codec
//! live here once rather than drifting apart in two copies.

use {
    crate::rpc::{
        constants::{MAX_TRANSACTION_V1_SIZE, PACKET_DATA_SIZE},
        error::{custom_error, INVALID_PARAMS_CODE},
    },
    jsonrpsee::types::ErrorObjectOwned,
    solana_sdk::transaction::VersionedTransaction,
};

/// Leading byte of a v1 message, which no legacy or v0 message can start with.
///
/// For legacy and v0 the first byte is the short-vec signature count, whose top
/// bit marks a versioned message and whose low bits hold the count, so the byte
/// is a reliable discriminator. The wincode reader branches on the same byte.
const V1_PREFIX: u8 = 0x81;

/// Largest submission this endpoint will read, decided by the leading byte.
///
/// Only v1 earned the bigger ceiling; legacy and v0 stay at the packet size,
/// which is what the network enforces for them.
fn max_size_for(tx_data: &[u8]) -> usize {
    match tx_data.first() {
        Some(&V1_PREFIX) => MAX_TRANSACTION_V1_SIZE,
        _ => PACKET_DATA_SIZE,
    }
}

/// Decode a submitted transaction, enforcing the size ceiling for its version.
///
/// Decoding uses wincode because that is the format signatures are taken over.
/// The serde encoding of a v1 message is a different layout that no sender
/// produces, so a bincode reader cannot accept v1 at any crate version.
pub fn decode_transaction(tx_data: &[u8]) -> Result<VersionedTransaction, ErrorObjectOwned> {
    let max_size = max_size_for(tx_data);
    if tx_data.len() > max_size {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!(
                "Transaction too large: {} bytes (max: {} bytes)",
                tx_data.len(),
                max_size
            ),
        ));
    }

    wincode::deserialize::<VersionedTransaction>(tx_data).map_err(|e| {
        custom_error(
            INVALID_PARAMS_CODE,
            format!("Failed to deserialize transaction: {}", e),
        )
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{
            hash::Hash,
            message::{
                compiled_instruction::CompiledInstruction, v0, v1, Message, MessageHeader,
                VersionedMessage,
            },
            pubkey::Pubkey,
            signature::Signature,
        },
    };

    /// A legacy transaction with `keys` account keys and `data_len` bytes of
    /// instruction data, used to grow a submission to an exact size.
    fn legacy_tx(keys: usize, data_len: usize) -> VersionedTransaction {
        let account_keys: Vec<Pubkey> = (0..keys).map(|_| Pubkey::new_unique()).collect();
        VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::Legacy(Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys,
                recent_blockhash: Hash::default(),
                instructions: vec![CompiledInstruction {
                    program_id_index: 0,
                    accounts: vec![],
                    data: vec![7u8; data_len],
                }],
            }),
        }
    }

    fn v0_tx() -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique()],
                recent_blockhash: Hash::default(),
                instructions: vec![CompiledInstruction {
                    program_id_index: 1,
                    accounts: vec![0],
                    data: vec![1, 2, 3],
                }],
                address_table_lookups: vec![],
            }),
        }
    }

    /// The codec swap must not change a single byte for the traffic the channel
    /// already carries, which is what makes it safe to land.
    #[test]
    fn wincode_matches_bincode_for_every_legacy_and_v0_shape() {
        let cases: Vec<(&str, VersionedTransaction)> = vec![
            ("legacy_small", legacy_tx(2, 3)),
            ("legacy_many_keys", legacy_tx(20, 64)),
            ("legacy_empty_data", legacy_tx(2, 0)),
            ("v0_no_lookups", v0_tx()),
        ];

        for (name, tx) in cases {
            let wincode_bytes = wincode::serialize(&tx).expect("wincode serialize");
            let bincode_bytes = bincode::serialize(&tx).expect("bincode serialize");
            assert_eq!(
                wincode_bytes, bincode_bytes,
                "{name}: wincode and bincode must agree byte for byte"
            );
        }
    }

    /// Anything the previous decoder accepted must still decode to the same
    /// transaction, otherwise the swap loses traffic.
    #[test]
    fn legacy_and_v0_round_trip_through_the_new_decoder() {
        for (name, tx) in [("legacy", legacy_tx(3, 8)), ("v0", v0_tx())] {
            let bytes = bincode::serialize(&tx).expect("serialize");
            let decoded = decode_transaction(&bytes).expect("decode");
            assert_eq!(decoded, tx, "{name} should survive a round trip");
        }
    }

    #[test]
    fn a_legacy_transaction_over_the_packet_size_is_refused() {
        let tx = legacy_tx(2, 2048);
        let bytes = bincode::serialize(&tx).expect("serialize");
        assert!(
            bytes.len() > PACKET_DATA_SIZE,
            "fixture must exceed the cap"
        );

        let err = decode_transaction(&bytes).expect_err("oversized legacy must be refused");
        assert_eq!(err.code(), INVALID_PARAMS_CODE);
        assert!(
            err.message().contains(&PACKET_DATA_SIZE.to_string()),
            "the error should name the legacy cap, got: {}",
            err.message()
        );
    }

    /// A v1 transaction big enough to be refused as legacy must decode, which is
    /// the whole point of choosing the ceiling from the leading byte.
    #[test]
    fn the_v1_prefix_raises_the_size_ceiling() {
        let account_keys: Vec<Pubkey> = (0..40).map(|_| Pubkey::new_unique()).collect();
        let message = v1::Message::new(
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            v1::TransactionConfig::empty(),
            Hash::default(),
            account_keys,
            vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![3u8; 64],
            }],
        );
        let tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V1(message),
        };

        let oversized_for_legacy = wincode::serialize(&tx).expect("wincode serialize");
        assert_eq!(
            oversized_for_legacy.first(),
            Some(&V1_PREFIX),
            "v1 must lead with 0x81"
        );
        assert!(
            oversized_for_legacy.len() > PACKET_DATA_SIZE,
            "fixture must exceed the legacy cap, got {} bytes",
            oversized_for_legacy.len()
        );

        let decoded = decode_transaction(&oversized_for_legacy)
            .expect("a v1 submission past the legacy cap must clear the size check");
        assert_eq!(decoded, tx);

        let past_v1_ceiling = vec![V1_PREFIX; MAX_TRANSACTION_V1_SIZE + 1];
        let err = decode_transaction(&past_v1_ceiling).expect_err("past the v1 ceiling");
        assert!(
            err.message().contains(&MAX_TRANSACTION_V1_SIZE.to_string()),
            "the error should name the v1 cap, got: {}",
            err.message()
        );
    }

    /// The whole point of the codec swap: a real v1 transaction, in the bytes a
    /// sender would actually put on the wire, decodes into a v1 message.
    #[test]
    fn a_real_v1_transaction_decodes() {
        let payer = Pubkey::new_unique();
        let message = v1::Message::new(
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            v1::TransactionConfig::empty(),
            Hash::default(),
            vec![payer, Pubkey::new_unique()],
            vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![9, 9, 9],
            }],
        );
        let tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V1(message),
        };

        let bytes = wincode::serialize(&tx).expect("wincode serialize");
        assert_eq!(bytes.first(), Some(&V1_PREFIX), "v1 must lead with 0x81");

        let decoded = decode_transaction(&bytes).expect("a v1 transaction must decode");
        assert!(matches!(decoded.message, VersionedMessage::V1(_)));
        assert_eq!(decoded, tx);
    }

    /// A v1 message carries its own compute budget, and the channel ignores it.
    /// Decoding must still preserve it, so the decision to ignore stays a policy
    /// at the execution boundary rather than an accident of the parser.
    #[test]
    fn a_v1_config_survives_decoding_even_though_execution_ignores_it() {
        let config = v1::TransactionConfig {
            priority_fee: Some(5_000),
            compute_unit_limit: Some(400_000),
            loaded_accounts_data_size_limit: Some(65_536),
            heap_size: None,
        };
        let message = v1::Message::new(
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            config.clone(),
            Hash::default(),
            vec![Pubkey::new_unique(), Pubkey::new_unique()],
            vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![1],
            }],
        );
        let tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V1(message),
        };

        let bytes = wincode::serialize(&tx).expect("wincode serialize");
        let decoded = decode_transaction(&bytes).expect("decode");
        match decoded.message {
            VersionedMessage::V1(decoded_message) => {
                assert_eq!(decoded_message.config, config, "the config must round trip");
            }
            other => panic!("expected a v1 message, got {other:?}"),
        }
    }

    /// Serde encodes a v1 message in a shape that is not the wire format, so a
    /// reader must not accept those bytes as though a sender had signed them.
    #[test]
    fn the_serde_encoding_of_v1_is_not_accepted_as_wire_bytes() {
        let message = v1::Message::new(
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            v1::TransactionConfig::empty(),
            Hash::default(),
            vec![Pubkey::new_unique(), Pubkey::new_unique()],
            vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![4, 5],
            }],
        );
        let tx = VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::V1(message),
        };

        let wire = wincode::serialize(&tx).expect("wincode serialize");
        let serde_bytes = bincode::serialize(&tx).expect("bincode serialize");
        assert_ne!(
            wire, serde_bytes,
            "the two encodings of v1 must differ, which is why the decoder moved"
        );
    }

    #[test]
    fn empty_input_is_refused_without_panicking() {
        let err = decode_transaction(&[]).expect_err("empty input must be refused");
        assert_eq!(err.code(), INVALID_PARAMS_CODE);
    }

    #[test]
    fn garbage_is_refused_without_panicking() {
        let err = decode_transaction(&[0xff; 64]).expect_err("garbage must be refused");
        assert_eq!(err.code(), INVALID_PARAMS_CODE);
    }

    /// Both codecs stop at the end of the message and ignore whatever follows it,
    /// so the swap neither tightened nor loosened what a submission may carry.
    #[test]
    fn trailing_bytes_are_ignored_exactly_as_bincode_ignored_them() {
        let tx = legacy_tx(2, 3);
        let mut padded = bincode::serialize(&tx).expect("serialize");
        padded.extend_from_slice(&[0u8; 8]);

        assert_eq!(decode_transaction(&padded).expect("wincode decodes"), tx);
        assert_eq!(
            bincode::deserialize::<VersionedTransaction>(&padded).expect("bincode decodes"),
            tx
        );
    }
}
