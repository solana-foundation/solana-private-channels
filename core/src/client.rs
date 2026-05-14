use {
    solana_hash::Hash,
    solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer, transaction::Transaction},
    spl_associated_token_account::get_associated_token_address,
    std::{fs, path::Path},
};

#[derive(Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

/// Create an SPL token transfer transaction
pub fn create_spl_transfer(
    from: &Keypair,
    to: &Pubkey,
    mint: &Pubkey,
    amount: u64,
    blockhash: Hash,
) -> Transaction {
    let from_pubkey = from.pubkey();
    let from_token_account = get_associated_token_address(&from_pubkey, mint);
    let to_token_account = get_associated_token_address(to, mint);

    Transaction::new_signed_with_payer(
        &[spl_token::instruction::transfer(
            &spl_token::id(),
            &from_token_account,
            &to_token_account,
            &from_pubkey,
            &[],
            amount,
        )
        .unwrap()],
        Some(&from_pubkey),
        &[from],
        blockhash,
    )
}

/// Create an SPL token burn transaction
pub fn create_spl_burn(from: &Keypair, mint: &Pubkey, amount: u64, blockhash: Hash) -> Transaction {
    let from_pubkey = from.pubkey();
    let from_token_account = get_associated_token_address(&from_pubkey, mint);

    Transaction::new_signed_with_payer(
        &[spl_token::instruction::burn(
            &spl_token::id(),
            &from_token_account,
            mint,
            &from_pubkey,
            &[],
            amount,
        )
        .unwrap()],
        Some(&from_pubkey),
        &[from],
        blockhash,
    )
}

/// Create a withdraw funds transaction (burns tokens and logs the event)
pub fn create_withdraw_funds(
    from: &Keypair,
    mint: &Pubkey,
    amount: u64,
    blockhash: Hash,
) -> Transaction {
    use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;

    let from_pubkey = from.pubkey();
    let token_account = get_associated_token_address(&from_pubkey, mint);

    let withdraw_ix = WithdrawFundsBuilder::new()
        .user(from_pubkey)
        .mint(*mint)
        .token_account(token_account)
        .token_program(spl_token::id())
        .associated_token_program(spl_associated_token_account::id())
        .amount(amount)
        .instruction();

    Transaction::new_signed_with_payer(&[withdraw_ix], Some(&from_pubkey), &[from], blockhash)
}

/// Create an admin transaction to initialize a mint
pub fn create_admin_initialize_mint(
    admin: &Keypair,
    mint: &Pubkey,
    decimals: u8,
    blockhash: Hash,
) -> Transaction {
    Transaction::new_signed_with_payer(
        &[spl_token::instruction::initialize_mint(
            &spl_token::id(),
            mint,
            &admin.pubkey(), // mint authority
            None,            // freeze authority
            decimals,
        )
        .unwrap()],
        Some(&admin.pubkey()),
        &[admin],
        blockhash,
    )
}

/// Create an admin transaction to mint tokens
pub fn create_admin_mint_to(
    admin: &Keypair,
    mint: &Pubkey,
    destination: &Pubkey,
    amount: u64,
    blockhash: Hash,
) -> Transaction {
    Transaction::new_signed_with_payer(
        &[spl_token::instruction::mint_to(
            &spl_token::id(),
            mint,
            destination,
            &admin.pubkey(),
            &[],
            amount,
        )
        .unwrap()],
        Some(&admin.pubkey()),
        &[admin],
        blockhash,
    )
}

/// Create a transaction to create an associated token account
pub fn create_ata_transaction(
    payer: &Keypair,
    owner: &Pubkey,
    mint: &Pubkey,
    blockhash: Hash,
) -> Transaction {
    Transaction::new_signed_with_payer(
        &[
            spl_associated_token_account::instruction::create_associated_token_account(
                &payer.pubkey(),
                owner,
                mint,
                &spl_token::id(),
            ),
        ],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    )
}

/// Load a keypair from a file
pub fn load_keypair(path: &Path) -> Result<Keypair, Box<dyn std::error::Error + Send + Sync>> {
    let keypair_string = fs::read_to_string(path)?;

    // Try to parse as JSON array of bytes
    let keypair_bytes: Vec<u8> = serde_json::from_str(&keypair_string)
        .map_err(|e| format!("Failed to parse keypair JSON: {}", e))?;

    // Solana keypairs are 64 bytes (32 bytes secret + 32 bytes public)
    if keypair_bytes.len() != 64 {
        return Err(format!(
            "Invalid keypair length: expected 64 bytes, got {}",
            keypair_bytes.len()
        )
        .into());
    }

    Keypair::try_from(keypair_bytes.as_slice())
        .map_err(|e| format!("Failed to create keypair: {}", e).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use spl_token::instruction::TokenInstruction;
    use spl_token::solana_program::program_option::COption;

    /// Helper: resolve the program ID from a compiled instruction
    fn resolve_program_id(tx: &Transaction, ix_index: usize) -> Pubkey {
        let ix = &tx.message.instructions[ix_index];
        tx.message.account_keys[ix.program_id_index as usize]
    }

    #[test]
    fn test_create_spl_transfer() {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let amount: u64 = 1_000;
        let blockhash = Hash::new_unique();

        let tx = create_spl_transfer(&from, &to, &mint, amount, blockhash);

        assert_eq!(tx.message.recent_blockhash, blockhash);
        assert_eq!(tx.message.instructions.len(), 1);
        assert_eq!(tx.message.account_keys[0], from.pubkey());

        // Program must be SPL Token
        assert_eq!(resolve_program_id(&tx, 0), spl_token::id());

        let ix = &tx.message.instructions[0];
        match TokenInstruction::unpack(&ix.data).expect("failed to unpack Transfer instruction") {
            TokenInstruction::Transfer {
                amount: decoded_amount,
            } => {
                assert_eq!(decoded_amount, amount);
            }
            other => panic!("expected Transfer, got {other:?}"),
        }

        // Account keys must contain the correct ATAs
        let expected_from_ata = get_associated_token_address(&from.pubkey(), &mint);
        let expected_to_ata = get_associated_token_address(&to, &mint);
        assert!(
            tx.message.account_keys.contains(&expected_from_ata),
            "source ATA missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&expected_to_ata),
            "destination ATA missing from account keys"
        );
    }

    #[test]
    fn test_create_spl_burn() {
        let from = Keypair::new();
        let mint = Pubkey::new_unique();
        let amount: u64 = 500;
        let blockhash = Hash::new_unique();

        let tx = create_spl_burn(&from, &mint, amount, blockhash);

        assert_eq!(tx.message.recent_blockhash, blockhash);
        assert_eq!(tx.message.instructions.len(), 1);
        assert_eq!(tx.message.account_keys[0], from.pubkey());
        assert_eq!(resolve_program_id(&tx, 0), spl_token::id());

        let ix = &tx.message.instructions[0];
        match TokenInstruction::unpack(&ix.data).expect("failed to unpack Burn instruction") {
            TokenInstruction::Burn {
                amount: decoded_amount,
            } => {
                assert_eq!(decoded_amount, amount);
            }
            other => panic!("expected Burn, got {other:?}"),
        }

        let expected_ata = get_associated_token_address(&from.pubkey(), &mint);
        assert!(
            tx.message.account_keys.contains(&expected_ata),
            "source ATA missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&mint),
            "mint missing from account keys"
        );
    }

    #[test]
    fn test_create_admin_initialize_mint() {
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let decimals: u8 = 6;
        let blockhash = Hash::new_unique();

        let tx = create_admin_initialize_mint(&admin, &mint, decimals, blockhash);

        assert_eq!(tx.message.recent_blockhash, blockhash);
        assert_eq!(tx.message.instructions.len(), 1);
        assert_eq!(tx.message.account_keys[0], admin.pubkey());
        assert_eq!(resolve_program_id(&tx, 0), spl_token::id());

        let ix = &tx.message.instructions[0];
        match TokenInstruction::unpack(&ix.data)
            .expect("failed to unpack InitializeMint instruction")
        {
            TokenInstruction::InitializeMint {
                decimals: decoded_decimals,
                mint_authority,
                freeze_authority,
            } => {
                assert_eq!(decoded_decimals, decimals);
                assert_eq!(mint_authority, admin.pubkey());
                assert_eq!(freeze_authority, COption::None);
            }
            other => panic!("expected InitializeMint, got {other:?}"),
        }

        assert!(
            tx.message.account_keys.contains(&mint),
            "mint missing from account keys"
        );
    }

    #[test]
    fn test_create_admin_mint_to() {
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let dest = Pubkey::new_unique();
        let amount: u64 = 1_000;
        let blockhash = Hash::new_unique();

        let tx = create_admin_mint_to(&admin, &mint, &dest, amount, blockhash);

        assert_eq!(tx.message.recent_blockhash, blockhash);
        assert_eq!(tx.message.instructions.len(), 1);
        assert_eq!(tx.message.account_keys[0], admin.pubkey());
        assert_eq!(resolve_program_id(&tx, 0), spl_token::id());

        let ix = &tx.message.instructions[0];
        match TokenInstruction::unpack(&ix.data).expect("failed to unpack MintTo instruction") {
            TokenInstruction::MintTo {
                amount: decoded_amount,
            } => {
                assert_eq!(decoded_amount, amount);
            }
            other => panic!("expected MintTo, got {other:?}"),
        }

        assert!(
            tx.message.account_keys.contains(&mint),
            "mint missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&dest),
            "destination missing from account keys"
        );
    }

    #[test]
    fn test_create_ata_transaction() {
        let payer = Keypair::new();
        let owner = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let blockhash = Hash::new_unique();

        let tx = create_ata_transaction(&payer, &owner, &mint, blockhash);

        assert_eq!(tx.message.recent_blockhash, blockhash);
        assert_eq!(tx.message.instructions.len(), 1);
        assert_eq!(tx.message.account_keys[0], payer.pubkey());

        // Program must be the Associated Token Account program
        assert_eq!(
            resolve_program_id(&tx, 0),
            spl_associated_token_account::id()
        );

        // The derived ATA address must appear in account keys
        let expected_ata = get_associated_token_address(&owner, &mint);
        assert!(
            tx.message.account_keys.contains(&expected_ata),
            "derived ATA missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&owner),
            "owner missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&mint),
            "mint missing from account keys"
        );
    }

    #[test]
    fn test_create_withdraw_funds() {
        let from = Keypair::new();
        let mint = Pubkey::new_unique();
        let amount: u64 = 500;
        let blockhash = Hash::new_unique();

        let tx = create_withdraw_funds(&from, &mint, amount, blockhash);

        assert_eq!(tx.message.recent_blockhash, blockhash);
        assert_eq!(tx.message.instructions.len(), 1);
        assert_eq!(tx.message.account_keys[0], from.pubkey());

        // Program must be the PrivateChannel withdraw program
        assert_eq!(
            resolve_program_id(&tx, 0),
            private_channel_withdraw_program_client::programs::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        );

        // Account keys must include the derived ATA, mint, and token programs
        let expected_ata = get_associated_token_address(&from.pubkey(), &mint);
        assert!(
            tx.message.account_keys.contains(&expected_ata),
            "token account ATA missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&mint),
            "mint missing from account keys"
        );
        assert!(
            tx.message.account_keys.contains(&spl_token::id()),
            "SPL Token program missing from account keys"
        );
        assert!(
            tx.message
                .account_keys
                .contains(&spl_associated_token_account::id()),
            "ATA program missing from account keys"
        );
    }

    // --- load_keypair tests ---

    #[test]
    fn test_load_keypair_valid() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes().to_vec();
        let json = serde_json::to_string(&bytes).unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &json).unwrap();

        let loaded = load_keypair(tmp.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn test_load_keypair_invalid_length_boundary() {
        // Test values around the 64-byte boundary
        for len in [0, 1, 32, 63, 65, 128] {
            let bytes: Vec<u8> = vec![0u8; len];
            let json = serde_json::to_string(&bytes).unwrap();

            let tmp = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(tmp.path(), &json).unwrap();

            let result = load_keypair(tmp.path());
            assert!(result.is_err(), "expected error for {len}-byte keypair");
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Invalid keypair length"),
                "expected 'Invalid keypair length' for {len} bytes, got: {err}"
            );
        }
    }

    #[test]
    fn test_load_keypair_invalid_json() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "not json at all").unwrap();

        let result = load_keypair(tmp.path());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("parse keypair JSON"),
            "expected JSON parse error, got: {err}"
        );
    }

    #[test]
    fn test_load_keypair_nonexistent_file() {
        let result = load_keypair(Path::new("/nonexistent/path/keypair.json"));
        assert!(result.is_err());
    }
}
