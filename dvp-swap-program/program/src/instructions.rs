extern crate alloc;

use codama::CodamaInstructions;
use pinocchio::Address as Pubkey;

/// Instructions for the DvP Swap Program.
///
/// Lifecycle: Create → fund each leg via raw SPL Transfer to the leg's
/// escrow ATA → Settle | (Reclaim per leg) | Cancel | Reject. Funding
/// is intentionally not a program instruction so that custodian
/// integrations can use a plain SPL Transfer.
/// Discriminator order matches `discriminator::DvpSwapInstructionDiscriminators`.
///
/// Token-2022: every transfer instruction accepts a separate token
/// program per leg (`token_program_a` / `token_program_b`), enabling
/// cross-program swaps (e.g. legacy-SPL ↔ Token-2022). Each token
/// program account must match the owner of its leg's mint. Mints
/// carrying amount-mutating Token-2022 extensions (ConfidentialTransfer,
/// TransferFee, InterestBearing, ScaledUiAmount) are rejected at
/// CreateDvp; later instructions do not re-check so that funds remain
/// recoverable if a mint's extension parameters change post-Create.
///
/// TransferHook is supported: instructions that issue a `TransferChecked`
/// CPI (Settle/Cancel/Reject/Reclaim) treat any accounts beyond their
/// fixed prefix as transfer-hook extras forwarded to the token program.
/// Settle/Cancel/Reject split the trailing accounts between the two legs
/// via the `leg_a_extras_count: u8` data field; Reclaim has a single
/// leg, so all trailing accounts feed its one CPI. The client is
/// responsible for resolving the hook's `ExtraAccountMetaList`
/// off-chain and supplying the resulting accounts in the order the hook
/// expects.
#[repr(C, u8)]
#[derive(Clone, Debug, PartialEq, CodamaInstructions)]
pub enum DvpSwapProgramInstruction {
    /// Permissionless. Creates the SwapDvp PDA and both escrow ATAs.
    /// No funding happens here; each leg is deposited by sending tokens
    /// via a raw SPL Transfer to the leg's escrow ATA.
    #[codama(account(
        name = "payer",
        docs = "Funds account/ATA creation rent",
        signer,
        writable
    ))]
    #[codama(account(name = "swap_dvp", docs = "SwapDvp PDA to be created", writable))]
    #[codama(account(
        name = "nonce_tombstone",
        docs = "Per-DvP nonce tombstone PDA, created here and never closed; rejects nonce reuse",
        writable
    ))]
    #[codama(account(
        name = "settlement_authority",
        docs = "Third-party authority allowed to settle/cancel; must not be executable so it can receive closed-account rent"
    ))]
    #[codama(account(name = "mint_a", docs = "Mint of the asset leg (seller delivers)"))]
    #[codama(account(name = "mint_b", docs = "Mint of the cash leg (buyer delivers)"))]
    #[codama(account(
        name = "dvp_ata_a",
        docs = "swap_dvp's ATA for mint_a (created here)",
        writable
    ))]
    #[codama(account(
        name = "dvp_ata_b",
        docs = "swap_dvp's ATA for mint_b (created here)",
        writable
    ))]
    #[codama(account(name = "system_program", docs = "System program"))]
    #[codama(account(
        name = "token_program_a",
        docs = "SPL Token or Token-2022 program; must own mint_a"
    ))]
    #[codama(account(
        name = "token_program_b",
        docs = "SPL Token or Token-2022 program; must own mint_b"
    ))]
    #[codama(account(
        name = "associated_token_program",
        docs = "Associated Token Account program"
    ))]
    CreateDvp {
        /// Seller; delivers `amount_a` of `mint_a`.
        user_a: Pubkey,
        /// Buyer; delivers `amount_b` of `mint_b`.
        user_b: Pubkey,
        /// Asset leg size.
        amount_a: u64,
        /// Cash leg size.
        amount_b: u64,
        /// Settlement is rejected after this Unix timestamp.
        expiry_timestamp: i64,
        /// Disambiguates DvPs sharing all other seeds.
        nonce: u64,
        /// If set, settlement is also rejected before this timestamp.
        earliest_settlement_timestamp: Option<i64>,
    } = 0,

    /// Permissioned: signer must be `dvp.user_a` or `dvp.user_b`. Drains
    /// the signer's leg back to the signer. The DvP stays open; the
    /// leg can be re-funded.
    #[codama(account(
        name = "signer",
        docs = "Depositor; must equal dvp.user_a or dvp.user_b",
        signer
    ))]
    #[codama(account(
        name = "swap_dvp",
        docs = "SwapDvp PDA (signs the transfer as authority)"
    ))]
    #[codama(account(
        name = "mint",
        docs = "Mint of the leg being reclaimed; must equal dvp.mint_a or dvp.mint_b"
    ))]
    #[codama(account(
        name = "dvp_source_ata",
        docs = "DvP's escrow ATA for the leg's mint",
        writable
    ))]
    #[codama(account(
        name = "signer_dest_ata",
        docs = "Signer's canonical ATA for the leg's mint",
        writable
    ))]
    #[codama(account(
        name = "token_program",
        docs = "SPL Token or Token-2022 program; must own mint"
    ))]
    #[codama(account(
        name = "memo_program",
        docs = "SPL Memo program; only used if signer_dest_ata requires a memo"
    ))]
    ReclaimDvp {} = 1,

    /// Permissioned: signer must be `dvp.settlement_authority`. Atomic
    /// DvP settlement — cash to seller, asset to buyer — followed by
    /// refunding any over-deposit surplus to each leg's depositor and
    /// closing both escrows and the SwapDvp PDA. Closed-account rent
    /// goes to the settlement authority.
    #[codama(account(
        name = "settlement_authority",
        docs = "Must equal dvp.settlement_authority; receives closed-account rent",
        signer,
        writable
    ))]
    #[codama(account(
        name = "swap_dvp",
        docs = "SwapDvp PDA (signs CPIs, then closed)",
        writable
    ))]
    #[codama(account(name = "mint_a", docs = "Must equal dvp.mint_a"))]
    #[codama(account(name = "mint_b", docs = "Must equal dvp.mint_b"))]
    #[codama(account(
        name = "dvp_ata_a",
        docs = "Asset escrow (drained, then closed)",
        writable
    ))]
    #[codama(account(
        name = "dvp_ata_b",
        docs = "Cash escrow (drained, then closed)",
        writable
    ))]
    #[codama(account(
        name = "user_a_ata_b",
        docs = "user_a's ATA for mint_b; receives the cash leg",
        writable
    ))]
    #[codama(account(
        name = "user_b_ata_a",
        docs = "user_b's ATA for mint_a; receives the asset leg",
        writable
    ))]
    #[codama(account(
        name = "user_a_ata_a",
        docs = "user_a's ATA for mint_a; receives any asset-leg surplus refund. Required and must be pre-initialized: anyone can dust the escrow, forcing a surplus refund, and a missing ATA reverts the whole Settle",
        writable
    ))]
    #[codama(account(
        name = "user_b_ata_b",
        docs = "user_b's ATA for mint_b; receives any cash-leg surplus refund. Required and must be pre-initialized, same as user_a_ata_a",
        writable
    ))]
    #[codama(account(
        name = "token_program_a",
        docs = "SPL Token or Token-2022 program; must own mint_a"
    ))]
    #[codama(account(
        name = "token_program_b",
        docs = "SPL Token or Token-2022 program; must own mint_b"
    ))]
    #[codama(account(
        name = "memo_program",
        docs = "SPL Memo program; only used for destinations that require a memo"
    ))]
    SettleDvp {
        /// Splits the trailing remaining accounts between the two
        /// legs' `TransferChecked` CPIs. The first
        /// `leg_a_extras_count` remaining accounts go to leg A; the
        /// rest go to leg B. Either value can be 0 independently
        /// (e.g. only leg B's mint carries a transfer hook).
        leg_a_extras_count: u8,
    } = 2,

    /// Permissioned: signer must be `dvp.settlement_authority`. Refunds
    /// any funded legs to their depositors and closes the trade. No
    /// expiry check — Cancel works post-expiry too. Closed-account rent
    /// goes to the settlement authority.
    #[codama(account(
        name = "settlement_authority",
        docs = "Must equal dvp.settlement_authority; receives closed-account rent",
        signer,
        writable
    ))]
    #[codama(account(
        name = "swap_dvp",
        docs = "SwapDvp PDA (signs CPIs, then closed)",
        writable
    ))]
    #[codama(account(name = "mint_a", docs = "Must equal dvp.mint_a"))]
    #[codama(account(name = "mint_b", docs = "Must equal dvp.mint_b"))]
    #[codama(account(
        name = "dvp_ata_a",
        docs = "Asset escrow (drained if funded, then closed)",
        writable
    ))]
    #[codama(account(
        name = "dvp_ata_b",
        docs = "Cash escrow (drained if funded, then closed)",
        writable
    ))]
    #[codama(account(
        name = "user_a_ata_a",
        docs = "user_a's ATA for mint_a; refund destination",
        writable
    ))]
    #[codama(account(
        name = "user_b_ata_b",
        docs = "user_b's ATA for mint_b; refund destination",
        writable
    ))]
    #[codama(account(
        name = "token_program_a",
        docs = "SPL Token or Token-2022 program; must own mint_a"
    ))]
    #[codama(account(
        name = "token_program_b",
        docs = "SPL Token or Token-2022 program; must own mint_b"
    ))]
    #[codama(account(
        name = "memo_program",
        docs = "SPL Memo program; only used for destinations that require a memo"
    ))]
    CancelDvp {
        /// Splits the trailing remaining accounts between the two
        /// legs' refund `TransferChecked` CPIs. The first
        /// `leg_a_extras_count` remaining accounts go to leg A; the
        /// rest go to leg B. Accounts for an unfunded leg are ignored
        /// since that leg's transfer is skipped.
        leg_a_extras_count: u8,
    } = 3,

    /// Permissioned: signer must be `dvp.user_a` or `dvp.user_b`. Either
    /// counterparty can pull the plug; refunds any funded legs to their
    /// depositors and closes the trade. Closed-account rent goes to the
    /// signer, not `dvp.settlement_authority` — Reject is the safety
    /// valve and must work even if the settlement authority is
    /// unreachable (e.g. a sysvar or executable address).
    #[codama(account(
        name = "signer",
        docs = "Must equal dvp.user_a or dvp.user_b; receives closed-account rent",
        signer,
        writable
    ))]
    #[codama(account(
        name = "swap_dvp",
        docs = "SwapDvp PDA (signs CPIs, then closed)",
        writable
    ))]
    #[codama(account(name = "mint_a", docs = "Must equal dvp.mint_a"))]
    #[codama(account(name = "mint_b", docs = "Must equal dvp.mint_b"))]
    #[codama(account(
        name = "dvp_ata_a",
        docs = "Asset escrow (drained if funded, then closed)",
        writable
    ))]
    #[codama(account(
        name = "dvp_ata_b",
        docs = "Cash escrow (drained if funded, then closed)",
        writable
    ))]
    #[codama(account(
        name = "user_a_ata_a",
        docs = "user_a's ATA for mint_a; refund destination",
        writable
    ))]
    #[codama(account(
        name = "user_b_ata_b",
        docs = "user_b's ATA for mint_b; refund destination",
        writable
    ))]
    #[codama(account(
        name = "token_program_a",
        docs = "SPL Token or Token-2022 program; must own mint_a"
    ))]
    #[codama(account(
        name = "token_program_b",
        docs = "SPL Token or Token-2022 program; must own mint_b"
    ))]
    #[codama(account(
        name = "memo_program",
        docs = "SPL Memo program; only used for destinations that require a memo"
    ))]
    RejectDvp {
        /// Splits the trailing remaining accounts between the two
        /// legs' refund `TransferChecked` CPIs. The first
        /// `leg_a_extras_count` remaining accounts go to leg A; the
        /// rest go to leg B. Accounts for an unfunded leg are ignored
        /// since that leg's transfer is skipped.
        leg_a_extras_count: u8,
    } = 4,
}
