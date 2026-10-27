use crate::accounts::precompiles::PRECOMPILES;
use agave_reserved_account_keys::ReservedAccountKeys;
use private_channel_withdraw_program_client::{
    instructions::SET_WITHDRAW_CONFIG_DISCRIMINATOR, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
};
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::SanitizedTransaction;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

/// The keys behind a channel mint's seize power: the permanent delegate written into every
/// mint, and the owner whose ATA is the only place the delegate may move balances to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeizePolicy {
    pub delegate: Option<Pubkey>,
    pub compliance_owner: Option<Pubkey>,
}

/// What sigverify needs to admit a transaction.
#[derive(Clone, Debug, Default)]
pub struct IngressPolicy {
    pub admin_keys: Vec<Pubkey>,
    pub seize: SeizePolicy,
}

const T22_INITIALIZE_MINT: u8 = 0;
const T22_INITIALIZE_MINT2: u8 = 20;
const T22_INITIALIZE_PERMANENT_DELEGATE: u8 = 35;

/// A lazy-initialized static mapping from program_id (Pubkey) to a HashSet of admin instruction types (u8)
pub static ADMIN_INSTRUCTIONS_MAP: LazyLock<HashMap<Pubkey, HashSet<u8>>> = LazyLock::new(|| {
    HashMap::from([(
        spl_token_2022::id(),
        HashSet::from([
            T22_INITIALIZE_MINT,
            T22_INITIALIZE_MINT2,
            T22_INITIALIZE_PERMANENT_DELEGATE,
        ]),
    )])
});

/// Checks if an instruction is an admin-only instruction
pub fn is_admin_instruction(program_id: &Pubkey, instruction_type: u8) -> bool {
    ADMIN_INSTRUCTIONS_MAP
        .get(program_id)
        .is_some_and(|set| set.contains(&instruction_type))
}

const T22_SET_AUTHORITY: u8 = 6;
const T22_MINT_TO: u8 = 7;
const T22_FREEZE_ACCOUNT: u8 = 10;
const T22_THAW_ACCOUNT: u8 = 11;
const T22_MINT_TO_CHECKED: u8 = 14;

/// `SetAuthority` authority types that belong to a token account owner.
const AUTHORITY_TYPE_ACCOUNT_OWNER: u8 = 2;
const AUTHORITY_TYPE_CLOSE_ACCOUNT: u8 = 3;
/// `SetAuthority` authority type of the mint's permanent delegate.
const AUTHORITY_TYPE_PERMANENT_DELEGATE: u8 = 8;

/// Instructions a mint's own authority signs: mint, freeze, thaw and mint-side
/// `SetAuthority`, and the withdraw config. They run as normal transactions, so a key
/// dropped from the admin set could still sign them. Requiring a configured admin signer
/// means that key can no longer use them alone.
pub fn requires_admin_signer(program_id: &Pubkey, data: &[u8]) -> bool {
    let Some(&instruction_type) = data.first() else {
        return false;
    };
    if *program_id == PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID {
        return instruction_type == SET_WITHDRAW_CONFIG_DISCRIMINATOR;
    }
    if *program_id != spl_token_2022::id() {
        return false;
    }
    match instruction_type {
        T22_MINT_TO | T22_FREEZE_ACCOUNT | T22_THAW_ACCOUNT | T22_MINT_TO_CHECKED => true,
        // Every mint-side type, including unknown ones (fail closed). AccountOwner and
        // CloseAccount stay open: they are a token account owner's own.
        T22_SET_AUTHORITY => !matches!(
            data.get(1),
            Some(&(AUTHORITY_TYPE_ACCOUNT_OWNER | AUTHORITY_TYPE_CLOSE_ACCOUNT))
        ),
        _ => false,
    }
}

/// True for a `SetAuthority(PermanentDelegate)` whose new authority is not `Some(delegate)`.
/// Rotating to an unconfigured key would leave seizes unpoliced, and None removes seize for good.
pub fn is_foreign_delegate_rotation(
    program_id: &Pubkey,
    data: &[u8],
    delegate: Option<&Pubkey>,
) -> bool {
    if *program_id != spl_token_2022::id()
        || data.first() != Some(&T22_SET_AUTHORITY)
        || data.get(1) != Some(&AUTHORITY_TYPE_PERMANENT_DELEGATE)
    {
        return false;
    }
    // Layout: [6, type, COption tag, new authority (32 bytes when the tag is 1)].
    let names_delegate =
        data.get(2) == Some(&1) && delegate.is_some_and(|d| data.get(3..35) == Some(d.as_ref()));
    !names_delegate
}

/// A Token-2022 balance move whose authority is the configured permanent delegate acting
/// on a source account it does not own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegateMove {
    pub instruction_type: u8,
    pub source: Pubkey,
    pub mint: Pubkey,
    /// Set for TransferChecked only.
    pub destination: Option<Pubkey>,
    pub amount: Option<u64>,
}

const T22_BURN: u8 = 8;
const T22_TRANSFER_CHECKED: u8 = 12;
const T22_BURN_CHECKED: u8 = 15;

/// The delegate move in one top-level instruction, if any. Token-2022 honours a
/// permanent delegate only on TransferChecked and the two burns (the others never read
/// it), and Batch and the fee/permissioned variants are refused at ingress, so these
/// three are the only seize paths. A source that is the delegate's own ATA is its own
/// balance, not a seize. `accounts` are the instruction's resolved account keys.
pub fn delegate_move(
    program_id: &Pubkey,
    accounts: &[Pubkey],
    data: &[u8],
    seize: &SeizePolicy,
) -> Option<DelegateMove> {
    let delegate = seize.delegate.as_ref()?;
    if *program_id != spl_token_2022::id() {
        return None;
    }
    let instruction_type = *data.first()?;
    // Layouts: TransferChecked [source, mint, destination, authority], burns [source, mint, authority].
    let authority_index = match instruction_type {
        T22_TRANSFER_CHECKED => 3,
        T22_BURN | T22_BURN_CHECKED => 2,
        _ => return None,
    };
    // Checked first so a normal transaction pays one key compare and no derivation.
    if accounts.get(authority_index) != Some(delegate) {
        return None;
    }
    let (source, mint) = (*accounts.first()?, *accounts.get(1)?);
    if source == token_2022_ata(delegate, &mint) {
        return None;
    }
    Some(DelegateMove {
        instruction_type,
        source,
        mint,
        destination: (instruction_type == T22_TRANSFER_CHECKED)
            .then(|| accounts.get(2).copied())
            .flatten(),
        amount: data
            .get(1..9)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_le_bytes),
    })
}

impl DelegateMove {
    /// Only a TransferChecked into the mint's compliance ATA may pass. The mint is the
    /// instruction's own, which Token-2022 checks against the source, so the derived
    /// compliance ATA is the real one. No compliance owner refuses every move.
    pub fn is_refused(&self, seize: &SeizePolicy) -> bool {
        let to_compliance = seize
            .compliance_owner
            .is_some_and(|owner| self.destination == Some(token_2022_ata(&owner, &self.mint)));
        !(self.instruction_type == T22_TRANSFER_CHECKED && to_compliance)
    }
}

fn token_2022_ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    spl_associated_token_account::get_associated_token_address_with_program_id(
        owner,
        mint,
        &spl_token_2022::id(),
    )
}

/// Token-2022 instructions a channel mint can use: the classic token set (0..=24) and
/// the permanent delegate init (35, admin only). Everything else is extension machinery
/// our mints do not carry, plus Batch (255) which would hide inner instructions from
/// the admin and seize checks, and the metadata and group interfaces.
fn is_allowed_token_2022_instruction(data: &[u8]) -> bool {
    matches!(
        data.first(),
        Some(0..=24 | &T22_INITIALIZE_PERMANENT_DELEGATE)
    )
}

/// Admission policy for a single top-level instruction. System is refused: SOL
/// has no use here, and a Transfer to a new address creates a permanent account
/// the gasless model never charges for. System still runs as a CPI target.
pub fn is_allowed_program_instruction(program_id: &Pubkey, data: &[u8]) -> bool {
    if *program_id == spl_token_2022::id() {
        return is_allowed_token_2022_instruction(data);
    }
    *program_id == spl_associated_token_account::id()
        || *program_id == spl_memo::id()
        || *program_id
            == private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID
        || *program_id == dvp_swap_program_client::DVP_SWAP_PROGRAM_ID
}

/// Rejection reason for a transaction with a top-level instruction outside the allowlist.
pub const PROGRAM_NOT_ALLOWED: &str =
    "Only Token-2022 (classic token instructions), ATA, Memo, Withdraw, and Swap program transactions are accepted";

/// Whether every top-level instruction of `tx` passes the admission policy.
pub fn all_instructions_allowed(tx: &SanitizedTransaction) -> bool {
    tx.message()
        .program_instructions_iter()
        .all(|(program_id, ix)| is_allowed_program_instruction(program_id, &ix.data))
}

/// Rejection reason for a transaction that lists the spl-token native mint.
pub const NATIVE_MINT_UNSUPPORTED: &str = "The native mint (wSOL) is not supported on this channel";

/// spl-token builds a native (wSOL) account without loading the mint, so refusing
/// any tx that lists it keeps fabricated gasless lamports from becoming wSOL. A CPI
/// can only reach listed keys, and lookup tables are refused before this runs.
pub fn lists_native_mint(tx: &SanitizedTransaction) -> bool {
    tx.message()
        .account_keys()
        .iter()
        .any(|key| *key == spl_token::native_mint::id())
}

/// Rejection reason for a writable key that no instruction is given.
pub const UNUSED_WRITABLE_ACCOUNT: &str =
    "Every writable account other than the fee payer must be passed to an instruction other than Memo";

/// The scheduler write-locks every writable key, so one no instruction uses would
/// serialize unrelated transactions on that account. A CPI can only reach accounts
/// its outer instruction was given, so forwarding still works. Memo never writes, so a
/// key only a Memo is given counts as unused.
pub fn has_unused_writable_account(tx: &SanitizedTransaction) -> bool {
    let message = tx.message();
    let used: HashSet<usize> = message
        .program_instructions_iter()
        .filter(|(program_id, _)| **program_id != spl_memo::id())
        .flat_map(|(_, ix)| ix.accounts.iter().map(|&index| usize::from(index)))
        .collect();
    // Index 0 is the fee payer, writable without being passed to an instruction.
    (1..message.account_keys().len())
        .any(|index| message.is_writable(index) && !used.contains(&index))
}

/// Every key the runtime reserves: sysvars and builtin programs.
static RESERVED_KEYS: LazyLock<HashSet<Pubkey>> =
    LazyLock::new(|| ReservedAccountKeys::all_keys_iter().copied().collect());

/// Rejection reason for a writable program or reserved key.
pub const WRITABLE_PROGRAM_OR_RESERVED_ACCOUNT: &str =
    "Program, sysvar and other reserved accounts must not be writable";

/// Honest txs read-lock the programs, system program and sysvars they list, so a write
/// lock on one serializes all of them. Admission sanitizes with no reserved keys, so
/// only a program the tx itself calls is demoted to read-only.
pub fn has_writable_program_or_reserved_account(tx: &SanitizedTransaction) -> bool {
    let message = tx.message();
    message
        .account_keys()
        .iter()
        .enumerate()
        .any(|(index, key)| {
            message.is_writable(index)
                && (PRECOMPILES.contains_key(key) || RESERVED_KEYS.contains(key))
        })
}

/// Rejection reason shared by every admission path, so clients see one wording.
pub const ADDRESS_LOOKUP_UNSUPPORTED: &str =
    "Address lookup tables are not supported; submit a transaction with no address table lookups";

/// True when a v0 message declares address table lookups.
///
/// No lookup table program is admitted here, so a declared lookup names a table
/// that cannot exist. Admitting one unresolved would leave the transaction's
/// account keys missing every address the lookup was supposed to supply.
pub fn has_address_table_lookups(message: &VersionedMessage) -> bool {
    // Exhaustive on purpose: a future message version must be classified here
    // rather than falling through to a silent false.
    match message {
        VersionedMessage::Legacy(_) => false,
        VersionedMessage::V0(m) => !m.address_table_lookups.is_empty(),
        // The v1 format has no lookup tables at all.
        VersionedMessage::V1(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;
    use solana_sdk::message::{v0, v0::MessageAddressTableLookup, Message, MessageHeader};
    use solana_system_interface::instruction::SystemInstruction;

    fn v0_message(num_lookups: usize) -> VersionedMessage {
        let address_table_lookups = (0..num_lookups)
            .map(|_| MessageAddressTableLookup {
                account_key: Pubkey::new_unique(),
                writable_indexes: vec![0],
                readonly_indexes: vec![],
            })
            .collect();
        VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![Pubkey::new_unique(), spl_token::id()],
            recent_blockhash: Hash::default(),
            instructions: vec![],
            address_table_lookups,
        })
    }

    // The predicate keys on declaration, not on whether an instruction uses the
    // table, and legacy messages have no lookups to declare.
    #[test]
    fn address_table_lookup_detection() {
        let cases = [
            (
                "legacy",
                VersionedMessage::Legacy(Message::default()),
                false,
            ),
            ("v0 with no lookups", v0_message(0), false),
            ("v0 with one lookup", v0_message(1), true),
            ("v0 with two lookups", v0_message(2), true),
        ];
        for (label, message, expected) in cases {
            assert_eq!(
                has_address_table_lookups(&message),
                expected,
                "{label} must report {expected}"
            );
        }
    }

    fn t22() -> Pubkey {
        spl_token_2022::id()
    }

    #[test]
    fn admin_instructions_are_the_token_2022_mint_creation_set() {
        assert_eq!(ADMIN_INSTRUCTIONS_MAP.len(), 1);
        assert_eq!(
            ADMIN_INSTRUCTIONS_MAP.get(&t22()),
            Some(&HashSet::from([0, 20, 35]))
        );
        for byte in 0..=u8::MAX {
            assert!(!is_admin_instruction(&spl_token::id(), byte));
        }
    }

    #[test]
    fn unknown_program_is_not_admin() {
        let random = Pubkey::new_unique();
        assert!(!is_admin_instruction(&random, 0));
    }

    /// Fail closed: only the classic token instruction set and the delegate init pass.
    #[test]
    fn token_2022_admits_exactly_bytes_0_to_24_and_35() {
        for byte in 0..=u8::MAX {
            let expected = byte <= 24 || byte == 35;
            assert_eq!(
                is_allowed_program_instruction(&t22(), &[byte, 0, 0, 0]),
                expected,
                "Token-2022 byte {byte}"
            );
        }
        assert!(!is_allowed_program_instruction(&t22(), &[]));
    }

    /// Extension machinery and the interface instructions are refused by construction.
    #[test]
    fn token_2022_refuses_batch_metadata_and_extension_instructions() {
        let refused: [(&str, Vec<u8>); 6] = [
            (
                "Batch wrapping MintTo",
                vec![255, 1, 9, 7, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            // spl-token-metadata-interface `Initialize` discriminator.
            (
                "metadata Initialize",
                vec![210, 225, 30, 162, 88, 184, 77, 141],
            ),
            ("CreateNativeMint", vec![31]),
            ("Reallocate", vec![29, 7, 0]),
            ("MemoTransferExtension", vec![30, 0]),
            ("WithdrawExcessLamports", vec![38]),
        ];
        for (label, data) in refused {
            assert!(!is_allowed_program_instruction(&t22(), &data), "{label}");
        }
    }

    #[test]
    fn spl_token_program_is_refused() {
        for data in [&[][..], &[3, 0][..], &[12][..]] {
            assert!(!is_allowed_program_instruction(&spl_token::id(), data));
        }
    }

    /// Mint-side authority use needs an admin signer: MintTo, Freeze, Thaw,
    /// MintToChecked, and SetAuthority of every type but AccountOwner and CloseAccount.
    #[test]
    fn admin_signer_table_for_every_byte_and_authority_type() {
        for byte in 0..=u8::MAX {
            for authority_type in (0..=18u8).map(Some).chain([None]) {
                let mut data = vec![byte];
                data.extend(authority_type);
                let expected = matches!(byte, 7 | 10 | 11 | 14)
                    || (byte == 6 && !matches!(authority_type, Some(2 | 3)));
                assert_eq!(
                    requires_admin_signer(&t22(), &data),
                    expected,
                    "byte {byte} authority type {authority_type:?}"
                );
            }
        }
        assert!(!requires_admin_signer(&t22(), &[]));
        assert!(!requires_admin_signer(&spl_token::id(), &[7]));
    }

    fn set_delegate_authority(new: Option<Pubkey>) -> Vec<u8> {
        spl_token_2022::instruction::set_authority(
            &t22(),
            &Pubkey::new_unique(),
            new.as_ref(),
            spl_token_2022::instruction::AuthorityType::PermanentDelegate,
            &Pubkey::new_unique(),
            &[],
        )
        .unwrap()
        .data
    }

    /// The on-chain delegate may only be rotated to the configured key.
    #[test]
    fn delegate_rotation_must_name_the_configured_delegate() {
        let delegate = Pubkey::new_unique();
        let to_delegate = set_delegate_authority(Some(delegate));
        assert!(!is_foreign_delegate_rotation(
            &t22(),
            &to_delegate,
            Some(&delegate)
        ));
        let refused = [
            (
                "other key",
                set_delegate_authority(Some(Pubkey::new_unique())),
            ),
            ("None strips seize", set_delegate_authority(None)),
            ("truncated", to_delegate[..20].to_vec()),
        ];
        for (label, data) in &refused {
            assert!(
                is_foreign_delegate_rotation(&t22(), data, Some(&delegate)),
                "{label}"
            );
        }
        // No configured delegate: every rotation is foreign.
        assert!(is_foreign_delegate_rotation(&t22(), &to_delegate, None));
    }

    /// Only a type-8 SetAuthority on Token-2022 is a delegate rotation.
    #[test]
    fn other_set_authority_types_are_not_delegate_rotations() {
        let delegate = Pubkey::new_unique();
        for authority_type in (0..=18u8).filter(|t| *t != 8) {
            let data = [6, authority_type, 1]
                .iter()
                .copied()
                .chain([7; 32])
                .collect::<Vec<_>>();
            assert!(!is_foreign_delegate_rotation(
                &t22(),
                &data,
                Some(&delegate)
            ));
        }
        let data = set_delegate_authority(None);
        assert!(!is_foreign_delegate_rotation(
            &spl_token::id(),
            &data,
            Some(&delegate)
        ));
        assert!(!is_foreign_delegate_rotation(
            &t22(),
            &[12, 8],
            Some(&delegate)
        ));
    }

    // ── Seize rule (`delegate_move`) ─────────────────────────────────────

    fn ata(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address_with_program_id(
            owner,
            mint,
            &t22(),
        )
    }

    struct Seize {
        policy: SeizePolicy,
        delegate: Pubkey,
        compliance: Pubkey,
        mint: Pubkey,
        alice_ata: Pubkey,
    }

    fn seize() -> Seize {
        let (delegate, compliance, mint) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        Seize {
            policy: SeizePolicy {
                delegate: Some(delegate),
                compliance_owner: Some(compliance),
            },
            delegate,
            compliance,
            mint,
            alice_ata: ata(&Pubkey::new_unique(), &mint),
        }
    }

    fn transfer_checked(
        source: &Pubkey,
        mint: &Pubkey,
        dest: &Pubkey,
        authority: &Pubkey,
    ) -> (Vec<Pubkey>, Vec<u8>) {
        let ix = spl_token_2022::instruction::transfer_checked(
            &t22(),
            source,
            mint,
            dest,
            authority,
            &[],
            500,
            6,
        )
        .unwrap();
        (ix.accounts.iter().map(|m| m.pubkey).collect(), ix.data)
    }

    fn burn(
        checked: bool,
        source: &Pubkey,
        mint: &Pubkey,
        authority: &Pubkey,
    ) -> (Vec<Pubkey>, Vec<u8>) {
        let ix = if checked {
            spl_token_2022::instruction::burn_checked(&t22(), source, mint, authority, &[], 500, 6)
        } else {
            spl_token_2022::instruction::burn(&t22(), source, mint, authority, &[], 500)
        }
        .unwrap();
        (ix.accounts.iter().map(|m| m.pubkey).collect(), ix.data)
    }

    fn refused(s: &Seize, (accounts, data): &(Vec<Pubkey>, Vec<u8>)) -> Option<bool> {
        delegate_move(&t22(), accounts, data, &s.policy).map(|m| m.is_refused(&s.policy))
    }

    /// Acting as delegate, only a TransferChecked into the mint's compliance ATA passes.
    #[test]
    fn delegate_moves_only_into_the_compliance_ata() {
        let s = seize();
        let compliance_ata = ata(&s.compliance, &s.mint);
        let other_mint = Pubkey::new_unique();
        let cases = [
            (
                "to compliance",
                transfer_checked(&s.alice_ata, &s.mint, &compliance_ata, &s.delegate),
                Some(false),
            ),
            (
                "to another ATA",
                transfer_checked(
                    &s.alice_ata,
                    &s.mint,
                    &ata(&Pubkey::new_unique(), &s.mint),
                    &s.delegate,
                ),
                Some(true),
            ),
            (
                "to the delegate",
                transfer_checked(
                    &s.alice_ata,
                    &s.mint,
                    &ata(&s.delegate, &s.mint),
                    &s.delegate,
                ),
                Some(true),
            ),
            (
                "to compliance of another mint",
                transfer_checked(
                    &s.alice_ata,
                    &s.mint,
                    &ata(&s.compliance, &other_mint),
                    &s.delegate,
                ),
                Some(true),
            ),
            (
                "Burn",
                burn(false, &s.alice_ata, &s.mint, &s.delegate),
                Some(true),
            ),
            (
                "BurnChecked",
                burn(true, &s.alice_ata, &s.mint, &s.delegate),
                Some(true),
            ),
        ];
        for (label, ix, expected) in cases {
            assert_eq!(refused(&s, &ix), expected, "{label}");
        }
    }

    /// The delegate's own ATA is its own balance (operator payouts), and other
    /// authorities are not the delegate: neither is a delegate move.
    #[test]
    fn own_balance_and_other_authorities_are_not_delegate_moves() {
        let s = seize();
        let own = ata(&s.delegate, &s.mint);
        let anywhere = ata(&Pubkey::new_unique(), &s.mint);
        let alice = Pubkey::new_unique();
        let cases = [
            (
                "own ATA TransferChecked",
                transfer_checked(&own, &s.mint, &anywhere, &s.delegate),
            ),
            ("own ATA Burn", burn(false, &own, &s.mint, &s.delegate)),
            (
                "owner TransferChecked",
                transfer_checked(&s.alice_ata, &s.mint, &anywhere, &alice),
            ),
            ("owner Burn", burn(true, &s.alice_ata, &s.mint, &alice)),
        ];
        for (label, ix) in cases {
            assert_eq!(refused(&s, &ix), None, "{label}");
        }
    }

    /// Token-2022 reads the permanent delegate only on TransferChecked and the burns,
    /// so the delegate signing Transfer or Approve is not a seize.
    #[test]
    fn delegate_as_transfer_or_approve_authority_is_not_a_delegate_move() {
        let s = seize();
        let dest = ata(&Pubkey::new_unique(), &s.mint);
        #[allow(deprecated)]
        let transfer =
            spl_token_2022::instruction::transfer(&t22(), &s.alice_ata, &dest, &s.delegate, &[], 5)
                .unwrap();
        let approve =
            spl_token_2022::instruction::approve(&t22(), &s.alice_ata, &dest, &s.delegate, &[], 5)
                .unwrap();
        for ix in [transfer, approve] {
            let accounts: Vec<Pubkey> = ix.accounts.iter().map(|m| m.pubkey).collect();
            assert_eq!(delegate_move(&t22(), &accounts, &ix.data, &s.policy), None);
        }
    }

    /// With no compliance owner configured every delegate move is refused.
    #[test]
    fn no_compliance_owner_refuses_every_delegate_move() {
        let mut s = seize();
        let compliance_ata = ata(&s.compliance, &s.mint);
        s.policy.compliance_owner = None;
        let ix = transfer_checked(&s.alice_ata, &s.mint, &compliance_ata, &s.delegate);
        assert_eq!(refused(&s, &ix), Some(true));
    }

    /// A truncated account list or another program is not a delegate move: the
    /// program itself refuses the former.
    #[test]
    fn truncated_or_foreign_instructions_are_not_delegate_moves() {
        let s = seize();
        let compliance_ata = ata(&s.compliance, &s.mint);
        let (accounts, data) =
            transfer_checked(&s.alice_ata, &s.mint, &compliance_ata, &s.delegate);
        assert_eq!(
            delegate_move(&t22(), &accounts[..3], &data, &s.policy),
            None
        );
        assert_eq!(
            delegate_move(&spl_token::id(), &accounts, &data, &s.policy),
            None
        );
        let no_delegate = SeizePolicy::default();
        assert_eq!(delegate_move(&t22(), &accounts, &data, &no_delegate), None);
    }

    /// The move carries what the alert logs.
    #[test]
    fn delegate_move_reports_mint_source_destination_and_amount() {
        let s = seize();
        let dest = ata(&s.compliance, &s.mint);
        let (accounts, data) = transfer_checked(&s.alice_ata, &s.mint, &dest, &s.delegate);
        assert_eq!(
            delegate_move(&t22(), &accounts, &data, &s.policy),
            Some(DelegateMove {
                instruction_type: 12,
                source: s.alice_ata,
                mint: s.mint,
                destination: Some(dest),
                amount: Some(500),
            })
        );
    }

    // ── Admission policy (`is_allowed_program_instruction`) ──────────────────

    fn system_id() -> Pubkey {
        solana_sdk_ids::system_program::ID
    }

    fn encode(ix: &SystemInstruction) -> Vec<u8> {
        bincode::serialize(ix).unwrap()
    }

    /// A1: System is refused, Transfer included, whatever the data.
    #[test]
    fn system_transfer_is_rejected() {
        let data = encode(&SystemInstruction::Transfer { lamports: 1 });
        for data in [&data[..], &[][..], &[2][..], &[2, 0, 0][..]] {
            assert!(
                !is_allowed_program_instruction(&system_id(), data),
                "System data {data:?} must be rejected"
            );
        }
    }

    // A2: serialize the real enum so every variant, present and future, is
    // pinned as refused.
    #[test]
    fn every_system_variant_is_rejected() {
        let owner = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let cases: [(&str, SystemInstruction); 13] = [
            ("Transfer", SystemInstruction::Transfer { lamports: 1 }),
            (
                "CreateAccount",
                SystemInstruction::CreateAccount {
                    lamports: 0,
                    space: 10 * 1024 * 1024,
                    owner,
                },
            ),
            ("Assign", SystemInstruction::Assign { owner }),
            (
                "CreateAccountWithSeed",
                SystemInstruction::CreateAccountWithSeed {
                    base,
                    seed: "seed".to_string(),
                    lamports: 0,
                    space: 10 * 1024 * 1024,
                    owner,
                },
            ),
            (
                "AdvanceNonceAccount",
                SystemInstruction::AdvanceNonceAccount,
            ),
            (
                "WithdrawNonceAccount",
                SystemInstruction::WithdrawNonceAccount(1),
            ),
            (
                "InitializeNonceAccount",
                SystemInstruction::InitializeNonceAccount(owner),
            ),
            (
                "AuthorizeNonceAccount",
                SystemInstruction::AuthorizeNonceAccount(owner),
            ),
            (
                "Allocate",
                SystemInstruction::Allocate {
                    space: 10 * 1024 * 1024,
                },
            ),
            (
                "AllocateWithSeed",
                SystemInstruction::AllocateWithSeed {
                    base,
                    seed: "seed".to_string(),
                    space: 10 * 1024 * 1024,
                    owner,
                },
            ),
            (
                "AssignWithSeed",
                SystemInstruction::AssignWithSeed {
                    base,
                    seed: "seed".to_string(),
                    owner,
                },
            ),
            (
                "TransferWithSeed",
                SystemInstruction::TransferWithSeed {
                    lamports: 1,
                    from_seed: "seed".to_string(),
                    from_owner: owner,
                },
            ),
            (
                "UpgradeNonceAccount",
                SystemInstruction::UpgradeNonceAccount,
            ),
        ];

        for (name, ix) in cases {
            let data = encode(&ix);
            assert!(
                !is_allowed_program_instruction(&system_id(), &data),
                "System::{name} must be rejected at ingress"
            );
        }
    }

    /// A6: non-System, non-token allowlisted programs are admitted for any instruction data.
    #[test]
    fn allowlisted_programs_admit_any_data() {
        let programs = [
            spl_associated_token_account::id(),
            spl_memo::id(),
            private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            dvp_swap_program_client::DVP_SWAP_PROGRAM_ID,
        ];
        for program_id in programs {
            for data in [&[][..], &[0xff; 8][..]] {
                assert!(
                    is_allowed_program_instruction(&program_id, data),
                    "allowlisted program {program_id} must stay admitted"
                );
            }
        }
    }

    /// A7: an unknown program is rejected whatever the data.
    #[test]
    fn unknown_program_is_rejected() {
        let random = Pubkey::new_unique();
        for data in [&[][..], &[2, 0, 0, 0][..], &[0xff; 8][..]] {
            assert!(!is_allowed_program_instruction(&random, data));
        }
    }
}
