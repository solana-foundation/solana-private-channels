//! Burn on the channel with `ConfidentialWithdrawFunds`; the operator releases the amount
//! into the destination's confidential balance on Solana.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    devnet_scripts::withdraw::run(true)
}
