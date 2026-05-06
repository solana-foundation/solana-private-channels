extern crate alloc;

use codama::CodamaInstructions;
use pinocchio::Address as Pubkey;

/// Instructions for the Contra Swap Program.
///
/// Lifecycle: Create → Fund (per leg) → Settle | (Reclaim per leg) | Cancel | Reject.
/// Discriminator order matches `discriminator::ContraSwapInstructionDiscriminators`.
#[repr(C, u8)]
#[derive(Clone, Debug, PartialEq, CodamaInstructions)]
pub enum ContraSwapProgramInstruction {
    /// Permissionless. Creates the SwapDvp PDA and both escrow ATAs.
    /// No funding happens here; legs are deposited via `FundDvp`.
    #[codama(account(
        name = "payer",
        docs = "Funds account/ATA creation rent",
        signer,
        writable
    ))]
    #[codama(account(name = "swap_dvp", docs = "SwapDvp PDA to be created", writable))]
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
    #[codama(account(name = "token_program", docs = "SPL Token program"))]
    #[codama(account(
        name = "associated_token_program",
        docs = "Associated Token Account program"
    ))]
    CreateDvp {
        /// Seller; delivers `amount_a` of `mint_a`.
        user_a: Pubkey,
        /// Buyer; delivers `amount_b` of `mint_b`.
        user_b: Pubkey,
        /// Only party allowed to settle. Also signs Cancel.
        settlement_authority: Pubkey,
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

    /// Permissioned: signer must be `dvp.user_a` or `dvp.user_b`. The
    /// signer's identity selects the leg: user_a deposits `amount_a` of
    /// `mint_a`; user_b deposits `amount_b` of `mint_b`. The leg's
    /// escrow must be empty (re-funding is rejected; reclaim first).
    #[codama(account(
        name = "signer",
        docs = "Depositor; must equal dvp.user_a or dvp.user_b",
        signer
    ))]
    #[codama(account(name = "swap_dvp", docs = "SwapDvp PDA owned by this program"))]
    #[codama(account(
        name = "signer_source_ata",
        docs = "Signer's canonical ATA for the leg's mint",
        writable
    ))]
    #[codama(account(
        name = "dvp_dest_ata",
        docs = "DvP's escrow ATA for the leg's mint",
        writable
    ))]
    #[codama(account(name = "token_program", docs = "SPL Token program"))]
    FundDvp {} = 1,

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
        name = "dvp_source_ata",
        docs = "DvP's escrow ATA for the leg's mint",
        writable
    ))]
    #[codama(account(
        name = "signer_dest_ata",
        docs = "Signer's canonical ATA for the leg's mint",
        writable
    ))]
    #[codama(account(name = "token_program", docs = "SPL Token program"))]
    ReclaimDvp {} = 2,

    /// Permissioned: signer must be `dvp.settlement_authority`. Atomic
    /// DvP settlement — cash to seller, asset to buyer — followed by
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
    #[codama(account(name = "token_program", docs = "SPL Token program"))]
    SettleDvp {} = 3,

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
    #[codama(account(name = "token_program", docs = "SPL Token program"))]
    CancelDvp {} = 4,

    /// Permissioned: signer must be `dvp.user_a` or `dvp.user_b`. Either
    /// counterparty can pull the plug; refunds any funded legs to their
    /// depositors and closes the trade. Closed-account rent goes to the
    /// rejecting signer.
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
    #[codama(account(name = "token_program", docs = "SPL Token program"))]
    RejectDvp {} = 5,
}
