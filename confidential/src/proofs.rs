//! Proofs for the operator's confidential releases and `MoveToPublic`, placed inline after
//! the escrow instruction in Token-2022's proof order.

use crate::{AeBytes, Ciphertext, Keys};
use bytemuck::Pod;
use solana_instruction::Instruction;
use solana_zk_elgamal_proof_interface::{instruction::ProofInstruction, proof_data::ZkProofData};
use solana_zk_sdk::encryption::elgamal::{ElGamalCiphertext, ElGamalPubkey};
use spl_token_confidential_transfer_proof_generation::{
    transfer::transfer_split_proof_data, transfer_with_fee::transfer_with_fee_split_proof_data,
    withdraw::withdraw_proof_data,
};

/// Why a proof could not be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProofError {
    #[error("a ciphertext or public key is not valid")]
    InvalidInput,
    #[error("the amount exceeds the available balance")]
    InsufficientBalance,
    #[error("the proof generator refused the inputs")]
    Generation,
}

/// The mint's confidential transfer fee, as the fee proofs need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseFee {
    pub basis_points: u16,
    pub maximum_fee: u64,
    pub withdraw_withheld_pubkey: [u8; 32],
}

/// A release's proof instructions and the fields the escrow forwards to Token-2022.
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseProofs {
    /// Verify instructions, to follow the escrow instruction in this order.
    pub instructions: Vec<Instruction>,
    /// The available balance ciphertext the proofs were built against.
    pub expected_available_balance: Ciphertext,
    pub new_decryptable_available_balance: AeBytes,
    pub auditor_ciphertext_lo: Ciphertext,
    pub auditor_ciphertext_hi: Ciphertext,
    pub equality_offset: i8,
    pub ciphertext_validity_offset: i8,
    pub range_offset: i8,
    pub fee_sigma_offset: i8,
    pub fee_ciphertext_validity_offset: i8,
}

/// `MoveToPublic`'s proof instructions and the new AES copy.
#[derive(Debug, Clone, PartialEq)]
pub struct MoveToPublicProofs {
    pub instructions: Vec<Instruction>,
    pub new_decryptable_available_balance: AeBytes,
    pub equality_offset: i8,
    pub range_offset: i8,
}

impl Keys {
    /// The AES copy of `amount` under the instance key.
    pub fn encrypt_aes(&self, amount: u64) -> AeBytes {
        self.aes.encrypt(amount).to_bytes()
    }

    /// Prove a release of `amount` from `available` (which must open to `balance`) to
    /// `destination`, for the mint's auditor and fee.
    pub fn prove_release(
        &self,
        available: &Ciphertext,
        balance: u64,
        amount: u64,
        destination: &[u8; 32],
        auditor: Option<&[u8; 32]>,
        fee: Option<&ReleaseFee>,
    ) -> Result<ReleaseProofs, ProofError> {
        let new_balance = balance
            .checked_sub(amount)
            .ok_or(ProofError::InsufficientBalance)?;
        let expected_available_balance = *available;
        let available = ElGamalCiphertext::from_bytes(available).ok_or(ProofError::InvalidInput)?;
        let destination = elgamal_pubkey(destination)?;
        let auditor = auditor.map(elgamal_pubkey).transpose()?;
        // The generators read the balance from an AES ciphertext.
        let decryptable = self.aes.encrypt(balance);
        let new_decryptable_available_balance = self.encrypt_aes(new_balance);
        let mut placed = Placed::default();

        let Some(fee) = fee else {
            let data = transfer_split_proof_data(
                &available,
                &decryptable,
                amount,
                &self.elgamal,
                &self.aes,
                &destination,
                auditor.as_ref(),
            )
            .map_err(|_| ProofError::Generation)?;
            let validity = &data.ciphertext_validity_proof_data_with_ciphertext;
            return Ok(ReleaseProofs {
                equality_offset: placed.place(
                    ProofInstruction::VerifyCiphertextCommitmentEquality,
                    &data.equality_proof_data,
                ),
                ciphertext_validity_offset: placed.place(
                    ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
                    &validity.proof_data,
                ),
                range_offset: placed.place(
                    ProofInstruction::VerifyBatchedRangeProofU128,
                    &data.range_proof_data,
                ),
                fee_sigma_offset: 0,
                fee_ciphertext_validity_offset: 0,
                expected_available_balance,
                new_decryptable_available_balance,
                auditor_ciphertext_lo: validity.ciphertext_lo.0,
                auditor_ciphertext_hi: validity.ciphertext_hi.0,
                instructions: placed.instructions,
            });
        };

        let withheld = elgamal_pubkey(&fee.withdraw_withheld_pubkey)?;
        let data = transfer_with_fee_split_proof_data(
            &available,
            &decryptable,
            amount,
            &self.elgamal,
            &self.aes,
            &destination,
            auditor.as_ref(),
            &withheld,
            fee.basis_points,
            fee.maximum_fee,
        )
        .map_err(|_| ProofError::Generation)?;
        let validity = &data.transfer_amount_ciphertext_validity_proof_data_with_ciphertext;
        // Placed in Token-2022's order: equality, validity, fee sigma, fee validity, range.
        let equality_offset = placed.place(
            ProofInstruction::VerifyCiphertextCommitmentEquality,
            &data.equality_proof_data,
        );
        let ciphertext_validity_offset = placed.place(
            ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
            &validity.proof_data,
        );
        let fee_sigma_offset = placed.place(
            ProofInstruction::VerifyPercentageWithCap,
            &data.percentage_with_cap_proof_data,
        );
        let fee_ciphertext_validity_offset = placed.place(
            ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity,
            &data.fee_ciphertext_validity_proof_data,
        );
        let range_offset = placed.place(
            ProofInstruction::VerifyBatchedRangeProofU256,
            &data.range_proof_data,
        );
        Ok(ReleaseProofs {
            equality_offset,
            ciphertext_validity_offset,
            range_offset,
            fee_sigma_offset,
            fee_ciphertext_validity_offset,
            expected_available_balance,
            new_decryptable_available_balance,
            auditor_ciphertext_lo: validity.ciphertext_lo.0,
            auditor_ciphertext_hi: validity.ciphertext_hi.0,
            instructions: placed.instructions,
        })
    }

    /// Prove moving `amount` from `available` (which must open to `balance`) to public.
    pub fn prove_move_to_public(
        &self,
        available: &Ciphertext,
        balance: u64,
        amount: u64,
    ) -> Result<MoveToPublicProofs, ProofError> {
        let new_balance = balance
            .checked_sub(amount)
            .ok_or(ProofError::InsufficientBalance)?;
        let available = ElGamalCiphertext::from_bytes(available).ok_or(ProofError::InvalidInput)?;
        let data = withdraw_proof_data(&available, balance, amount, &self.elgamal)
            .map_err(|_| ProofError::Generation)?;
        let mut placed = Placed::default();
        Ok(MoveToPublicProofs {
            equality_offset: placed.place(
                ProofInstruction::VerifyCiphertextCommitmentEquality,
                &data.equality_proof_data,
            ),
            range_offset: placed.place(
                ProofInstruction::VerifyBatchedRangeProofU64,
                &data.range_proof_data,
            ),
            new_decryptable_available_balance: self.encrypt_aes(new_balance),
            instructions: placed.instructions,
        })
    }
}

fn elgamal_pubkey(bytes: &[u8; 32]) -> Result<ElGamalPubkey, ProofError> {
    ElGamalPubkey::try_from(bytes.as_slice()).map_err(|_| ProofError::InvalidInput)
}

/// Inline proofs; each offset is the proof's position after the escrow instruction.
#[derive(Default)]
struct Placed {
    instructions: Vec<Instruction>,
}

impl Placed {
    fn place<T: Pod + ZkProofData<U>, U: Pod>(&mut self, kind: ProofInstruction, proof: &T) -> i8 {
        self.instructions
            .push(kind.encode_verify_proof(None, proof));
        self.instructions.len() as i8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn destination() -> [u8; 32] {
        other_keys().pubkey()
    }

    #[test]
    fn release_proofs_without_fee_are_three_in_order() {
        let keys = keys();
        let available = encrypt(&keys, 1_000);
        let proofs = keys
            .prove_release(&available, 1_000, 300, &destination(), None, None)
            .unwrap();
        assert_eq!(proofs.instructions.len(), 3);
        assert_eq!(
            (
                proofs.equality_offset,
                proofs.ciphertext_validity_offset,
                proofs.range_offset
            ),
            (1, 2, 3)
        );
        assert_eq!(
            (
                proofs.fee_sigma_offset,
                proofs.fee_ciphertext_validity_offset
            ),
            (0, 0)
        );
        assert_eq!(
            keys.decrypt_aes(&proofs.new_decryptable_available_balance),
            Some(700)
        );
        for ix in &proofs.instructions {
            assert_eq!(ix.program_id, solana_zk_elgamal_proof_interface::id());
            assert!(ix.accounts.is_empty(), "inline proofs take no accounts");
        }
    }

    #[test]
    fn release_proofs_name_the_available_balance_they_were_built_against() {
        let keys = keys();
        let available = encrypt(&keys, 1_000);
        let proofs = keys
            .prove_release(&available, 1_000, 300, &destination(), None, None)
            .unwrap();
        assert_eq!(proofs.expected_available_balance, available);
    }

    #[test]
    fn release_auditor_ciphertexts_carry_the_amount() {
        let keys = keys();
        let auditor = other_keys();
        let proofs = keys
            .prove_release(
                &encrypt(&keys, 70_000),
                70_000,
                70_000,
                &destination(),
                Some(&auditor.pubkey()),
                None,
            )
            .unwrap();
        // The auditor's grouped handle is the third; the escrow event decrypts lo and hi.
        assert_eq!(
            auditor.decrypt_release(&proofs.auditor_ciphertext_lo, &proofs.auditor_ciphertext_hi),
            Some(70_000)
        );
    }

    #[test]
    fn release_proofs_with_fee_are_five_in_token_2022_order() {
        let keys = keys();
        let fee = ReleaseFee {
            basis_points: 100,
            maximum_fee: 5_000,
            withdraw_withheld_pubkey: other_keys().pubkey(),
        };
        let proofs = keys
            .prove_release(
                &encrypt(&keys, 10_000),
                10_000,
                4_000,
                &destination(),
                None,
                Some(&fee),
            )
            .unwrap();
        assert_eq!(proofs.instructions.len(), 5);
        assert_eq!(
            (
                proofs.equality_offset,
                proofs.ciphertext_validity_offset,
                proofs.fee_sigma_offset,
                proofs.fee_ciphertext_validity_offset,
                proofs.range_offset
            ),
            (1, 2, 3, 4, 5)
        );
        assert_eq!(
            keys.decrypt_aes(&proofs.new_decryptable_available_balance),
            Some(6_000)
        );
    }

    #[test]
    fn release_refuses_more_than_the_balance() {
        let keys = keys();
        assert_eq!(
            keys.prove_release(&encrypt(&keys, 10), 10, 11, &destination(), None, None),
            Err(ProofError::InsufficientBalance)
        );
    }

    #[test]
    fn release_refuses_a_bad_destination_key() {
        let keys = keys();
        assert_eq!(
            keys.prove_release(&encrypt(&keys, 10), 10, 1, &[0xff; 32], None, None),
            Err(ProofError::InvalidInput)
        );
    }

    #[test]
    fn move_to_public_proofs_are_equality_then_range() {
        let keys = keys();
        let proofs = keys
            .prove_move_to_public(&encrypt(&keys, 500), 500, 200)
            .unwrap();
        assert_eq!(proofs.instructions.len(), 2);
        assert_eq!((proofs.equality_offset, proofs.range_offset), (1, 2));
        assert_eq!(
            keys.decrypt_aes(&proofs.new_decryptable_available_balance),
            Some(300)
        );
        assert_eq!(
            keys.prove_move_to_public(&encrypt(&keys, 5), 5, 6),
            Err(ProofError::InsufficientBalance)
        );
    }

    #[test]
    fn encrypt_aes_round_trips() {
        let keys = keys();
        assert_eq!(keys.decrypt_aes(&keys.encrypt_aes(42)), Some(42));
    }
}
