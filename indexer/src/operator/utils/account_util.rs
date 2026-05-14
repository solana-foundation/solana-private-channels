use private_channel_escrow_program_client::{
    programs::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, Instance,
};
use solana_sdk::pubkey::Pubkey;

const INSTANCE_SEED: &[u8] = b"instance";
const EVENT_AUTHORITY_SEED: &[u8] = b"event_authority";
const ALLOWED_MINT_SEED: &[u8] = b"allowed_mint";
const OPERATOR_SEED: &[u8] = b"operator";

pub fn find_instance_pda(instance_seed: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[INSTANCE_SEED, instance_seed.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

pub fn find_event_authority_pda() -> Pubkey {
    Pubkey::find_program_address(&[EVENT_AUTHORITY_SEED], &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).0
}

pub fn find_allowed_mint_pda(instance_pda: &Pubkey, mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[ALLOWED_MINT_SEED, instance_pda.as_ref(), mint.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

pub fn find_operator_pda(instance_pda: &Pubkey, wallet: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[OPERATOR_SEED, instance_pda.as_ref(), wallet.as_ref()],
        &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
    )
    .0
}

pub fn parse_instance(instance_data: &[u8]) -> Result<Instance, std::io::Error> {
    Instance::from_bytes(instance_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(i: u8) -> Pubkey {
        let mut b = [0u8; 32];
        b[0] = i;
        Pubkey::new_from_array(b)
    }

    #[test]
    fn find_instance_pda_deterministic() {
        let seed = pk(1);
        let pda1 = find_instance_pda(&seed);
        let pda2 = find_instance_pda(&seed);
        assert_eq!(pda1, pda2);
    }

    #[test]
    fn find_event_authority_pda_non_default() {
        let pda = find_event_authority_pda();
        assert_ne!(pda, Pubkey::default());
    }

    #[test]
    fn find_allowed_mint_pda_different_mints_different_pdas() {
        let instance = pk(1);
        let pda_a = find_allowed_mint_pda(&instance, &pk(2));
        let pda_b = find_allowed_mint_pda(&instance, &pk(3));
        assert_ne!(pda_a, pda_b);
    }

    #[test]
    fn find_operator_pda_different_wallets_different_pdas() {
        let instance = pk(1);
        let pda_a = find_operator_pda(&instance, &pk(10));
        let pda_b = find_operator_pda(&instance, &pk(11));
        assert_ne!(pda_a, pda_b);
    }

    #[test]
    fn parse_instance_empty_bytes_errors() {
        let result = parse_instance(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_instance_short_bytes_errors() {
        let result = parse_instance(&[1, 2, 3]);
        assert!(result.is_err());
    }
}
