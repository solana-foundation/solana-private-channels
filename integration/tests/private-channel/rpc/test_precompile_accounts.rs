use private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID;
use {
    super::test_context::PrivateChannelContext,
    solana_sdk::{account::ReadableAccount, pubkey::Pubkey},
};

struct TestPrecompile {
    pubkey: Pubkey,
    pub name: &'static str,
    pub executable: bool,
    pub expect_empty_data: bool,
}

/// Test that precompile accounts are accessible via getAccountInfo
pub async fn run_precompile_accounts_test(private_channel_ctx: &PrivateChannelContext) {
    println!("\n=== Testing Precompile Account Info ===");

    // List of precompiles that should be available
    let precompiles = vec![
        TestPrecompile {
            pubkey: solana_sdk_ids::system_program::ID,
            name: "System Program",
            executable: true,
            expect_empty_data: false,
        },
        TestPrecompile {
            pubkey: spl_token::ID,
            name: "SPL Token Program",
            executable: true,
            expect_empty_data: false,
        },
        TestPrecompile {
            pubkey: spl_associated_token_account::ID,
            name: "Associated Token Account Program",
            executable: true,
            expect_empty_data: false,
        },
        TestPrecompile {
            pubkey: solana_sdk_ids::sysvar::rent::ID,
            name: "Rent Sysvar",
            executable: false,
            expect_empty_data: false,
        },
        TestPrecompile {
            pubkey: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            name: "PrivateChannel Withdraw Program",
            executable: true,
            expect_empty_data: false,
        },
    ];

    for precompile in precompiles {
        println!("  Testing {}: {}", precompile.name, precompile.pubkey);

        // Get account info
        let account = private_channel_ctx
            .read_client
            .get_account(&precompile.pubkey)
            .await;
        println!("Account info: {:?}", account);

        let account = match account {
            Ok(account) => account,
            Err(e) => panic!(
                "Failed to get account info for {}: {:?}",
                precompile.name, e
            ),
        };

        // Verify account exists and has expected properties
        assert!(
            account.lamports() > 0
                || precompile.name == "System Program"
                || precompile.name == "Rent Sysvar",
            "{} should have lamports or be a special account",
            precompile.name
        );

        // Verify executable for programs
        assert_eq!(
            precompile.executable,
            account.executable(),
            "{} should be marked as executable={}, got={}",
            precompile.name,
            precompile.executable,
            account.executable()
        );

        // Verify data is not empty for most precompiles
        assert_eq!(
            precompile.expect_empty_data,
            account.data().is_empty(),
            "{} should have empty data={}, got={}",
            precompile.name,
            precompile.expect_empty_data,
            account.data().is_empty()
        );

        println!("    ✓ {} is accessible and valid", precompile.name);
    }

    println!("=== Precompile Account Info Test Passed ===\n");
}
