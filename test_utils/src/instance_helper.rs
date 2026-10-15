use base64::{engine::general_purpose::STANDARD, Engine as _};
use private_channel_escrow_program_client::{
    programs::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, Instance,
};
use private_channel_indexer::operator::utils::find_instance_pda;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;

const TEST_INSTANCE_SEED: Pubkey = Pubkey::new_from_array([7; 32]);

/// An instance address the startup instance check accepts when a mock RPC serves
/// `test_instance_account()` for it: the PDA of a fixed seed.
pub fn test_instance() -> Pubkey {
    find_instance_pda(&TEST_INSTANCE_SEED)
}

/// The `getAccountInfo` value of `test_instance()`, a live escrow instance.
pub fn test_instance_account() -> Value {
    let data = borsh::to_vec(&Instance {
        discriminator: 0,
        bump: 255,
        version: 1,
        instance_seed: TEST_INSTANCE_SEED,
        admin: Pubkey::new_unique(),
    })
    .unwrap();
    json!({
        "data": [STANDARD.encode(&data), "base64"],
        "executable": false,
        "lamports": 1_000_000,
        "owner": PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string(),
        "rentEpoch": 0,
        "space": data.len(),
    })
}
