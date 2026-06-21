use pinocchio::ProgramResult;

use crate::{
    constants::tree_constants::{MAX_TREE_LEAVES, TREE_HEIGHT},
    constants::{EMPTY_LEAF, NON_EMPTY_LEAF_HASH},
    error::PrivateChannelEscrowProgramError,
};

pub struct SparseMerkleTreeUtils;

impl SparseMerkleTreeUtils {
    fn safe_sha256(input: &[u8]) -> [u8; 32] {
        const_crypto::sha2::Sha256::new().update(input).finalize()
    }

    /// Hash two 32-byte values together
    fn hash_combine(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(left);
        combined[32..].copy_from_slice(right);

        Self::safe_sha256(&combined)
    }

    /// Verifies SMT exclusion proof - proves that a transaction nonce does NOT exist in the SMT
    ///
    /// # Arguments
    /// * `current_root` - Current SMT root hash
    /// * `transaction_nonce` - Transaction nonce to prove exclusion of
    /// * `sibling_proofs` - Array of sibling hashes for the merkle path (max 16 levels)
    ///
    /// # SMT Algorithm
    /// 1. Use transaction nonce bits directly for path determination
    /// 2. For each bit in the nonce (from LSB to MSB), determine path direction
    /// 3. Compute root hash by traversing up the tree using sibling proofs
    /// 4. Verify computed root matches current root (exclusion proof valid)
    pub fn verify_smt_exclusion_proof(
        current_root: &[u8; 32],
        transaction_nonce: u64,
        sibling_proofs: &[[u8; 32]; TREE_HEIGHT],
    ) -> ProgramResult {
        // Use nonce bits directly for path determination
        let leaf_position = transaction_nonce as usize % MAX_TREE_LEAVES;

        // Start with empty leaf (null hash for exclusion proof)
        let mut current_hash = EMPTY_LEAF;

        // Traverse from leaf to root using sibling proofs
        for (level, &sibling) in sibling_proofs.iter().enumerate() {
            // Determine path direction based on bit at current level
            let bit = (leaf_position >> level) & 1;

            // Compute parent hash based on path direction
            current_hash = if bit == 0 {
                // Left child: hash(current, sibling)
                Self::hash_combine(&current_hash, &sibling)
            } else {
                // Right child: hash(sibling, current)
                Self::hash_combine(&sibling, &current_hash)
            };

            // Early termination: if we reach current root, exclusion proof is valid
            if current_hash == *current_root {
                return Ok(());
            }
        }

        // Final check: computed root must match current root for valid exclusion
        if current_hash != *current_root {
            return Err(PrivateChannelEscrowProgramError::InvalidSmtProof.into());
        }

        Ok(())
    }

    /// Verifies SMT inclusion proof - proves that a transaction nonce DOES exist in the SMT
    ///
    /// # Arguments
    /// * `new_root` - New SMT root hash after inclusion
    /// * `transaction_nonce` - Transaction nonce to prove inclusion of
    /// * `sibling_proofs` - Array of sibling hashes for the merkle path (max 16 levels)
    ///
    /// # SMT Algorithm
    /// 1. Use transaction nonce bits directly for path determination
    /// 2. For each bit in the nonce (from LSB to MSB), determine path direction
    /// 3. Compute root hash by traversing up the tree using sibling proofs
    /// 4. Start with non-empty leaf (constant hash for inclusion)
    /// 5. Verify computed root matches new root (inclusion proof valid)
    pub fn verify_smt_inclusion_proof(
        new_root: &[u8; 32],
        transaction_nonce: u64,
        sibling_proofs: &[[u8; 32]; TREE_HEIGHT],
    ) -> ProgramResult {
        // Use nonce bits directly for path determination
        let leaf_position = transaction_nonce as usize % MAX_TREE_LEAVES;

        // Start with non-empty leaf (what's actually stored in the SMT for inclusion proof)
        let mut current_hash = NON_EMPTY_LEAF_HASH;

        // Traverse from leaf to root using sibling proofs
        for (level, &sibling) in sibling_proofs.iter().enumerate() {
            // Determine path direction based on bit at current level
            let bit = (leaf_position >> level) & 1;

            // Compute parent hash based on path direction
            current_hash = if bit == 0 {
                // Left child: hash(current, sibling)
                Self::hash_combine(&current_hash, &sibling)
            } else {
                // Right child: hash(sibling, current)
                Self::hash_combine(&sibling, &current_hash)
            };

            // Early termination: if we reach new root, inclusion proof is valid
            if current_hash == *new_root {
                return Ok(());
            }
        }

        // Final check: computed root must match new root for valid inclusion
        if current_hash != *new_root {
            return Err(PrivateChannelEscrowProgramError::InvalidSmtProof.into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to create test hash from u32
    fn test_hash(val: u32) -> [u8; 32] {
        let mut hash = [0u8; 32];
        hash[..4].copy_from_slice(&val.to_le_bytes());
        hash
    }

    /// Helper to compute expected root manually following same path as verification
    fn compute_expected_root(
        transaction_nonce: u64,
        sibling_proofs: &[[u8; 32]; TREE_HEIGHT],
    ) -> [u8; 32] {
        let leaf_position = transaction_nonce as usize % MAX_TREE_LEAVES;
        let mut current_hash = [0u8; 32]; // Start with empty leaf

        for (level, &sibling) in sibling_proofs.iter().enumerate() {
            let bit = (leaf_position >> level) & 1;
            current_hash = if bit == 0 {
                SparseMerkleTreeUtils::hash_combine(&current_hash, &sibling)
            } else {
                SparseMerkleTreeUtils::hash_combine(&sibling, &current_hash)
            };
        }

        current_hash
    }

    /// Helper to compute expected inclusion root manually following same path as verification
    fn compute_expected_inclusion_root(
        transaction_nonce: u64,
        sibling_proofs: &[[u8; 32]; TREE_HEIGHT],
    ) -> [u8; 32] {
        let leaf_position = transaction_nonce as usize % MAX_TREE_LEAVES;
        let mut current_hash = NON_EMPTY_LEAF_HASH; // Start with actual leaf value for inclusion

        for (level, &sibling) in sibling_proofs.iter().enumerate() {
            let bit = (leaf_position >> level) & 1;
            current_hash = if bit == 0 {
                SparseMerkleTreeUtils::hash_combine(&current_hash, &sibling)
            } else {
                SparseMerkleTreeUtils::hash_combine(&sibling, &current_hash)
            };
        }

        current_hash
    }

    #[test]
    fn test_hash_combine_deterministic() {
        // Test that hash combination is deterministic
        let left = test_hash(123);
        let right = test_hash(456);
        let combined1 = SparseMerkleTreeUtils::hash_combine(&left, &right);
        let combined2 = SparseMerkleTreeUtils::hash_combine(&left, &right);
        assert_eq!(combined1, combined2);
    }

    #[test]
    fn test_hash_combine_order_matters() {
        // Test that hash combination is order-sensitive
        let left = test_hash(123);
        let right = test_hash(456);
        let combined1 = SparseMerkleTreeUtils::hash_combine(&left, &right);
        let combined2 = SparseMerkleTreeUtils::hash_combine(&right, &left);
        assert_ne!(combined1, combined2);
    }

    #[test]
    fn test_hash_combine_avalanche_effect() {
        // Small change in input should produce significantly different output
        let left1 = test_hash(123);
        let left2 = test_hash(124); // Only 1 bit difference
        let right = test_hash(456);

        let combined1 = SparseMerkleTreeUtils::hash_combine(&left1, &right);
        let combined2 = SparseMerkleTreeUtils::hash_combine(&left2, &right);

        // Count differing bits
        let mut diff_bits = 0;
        for i in 0..32 {
            diff_bits += (combined1[i] ^ combined2[i]).count_ones();
        }

        // Should have good avalanche effect (roughly 50% bits different)
        assert!(
            diff_bits > 50,
            "Poor avalanche effect: only {} bits different",
            diff_bits
        );
    }

    #[test]
    fn test_verify_smt_exclusion_proof_empty_tree() {
        // Test exclusion proof against completely empty tree
        let transaction_nonce = 42u64;
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT]; // All empty siblings
        let expected_root = compute_expected_root(transaction_nonce, &sibling_proofs);

        let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &expected_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_smt_exclusion_proof_different_nonces() {
        // Test that different nonces produce different paths/roots
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        let nonce1 = 100u64;
        let nonce2 = 200u64;

        let root1 = compute_expected_root(nonce1, &sibling_proofs);
        let root2 = compute_expected_root(nonce2, &sibling_proofs);

        // Should be different unless hash collision (extremely unlikely)
        assert_ne!(root1, root2);

        // Each should verify against its own root
        assert!(
            SparseMerkleTreeUtils::verify_smt_exclusion_proof(&root1, nonce1, &sibling_proofs)
                .is_ok()
        );
        assert!(
            SparseMerkleTreeUtils::verify_smt_exclusion_proof(&root2, nonce2, &sibling_proofs)
                .is_ok()
        );

        // But not against the other's root
        assert!(
            SparseMerkleTreeUtils::verify_smt_exclusion_proof(&root1, nonce2, &sibling_proofs)
                .is_err()
        );
        assert!(
            SparseMerkleTreeUtils::verify_smt_exclusion_proof(&root2, nonce1, &sibling_proofs)
                .is_err()
        );
    }

    #[test]
    fn test_verify_smt_exclusion_proof_with_siblings() {
        // Test with non-empty sibling proofs
        let transaction_nonce = 1337u64;
        let mut sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        // Set some non-zero sibling values
        sibling_proofs[0] = test_hash(111);
        sibling_proofs[1] = test_hash(222);
        sibling_proofs[TREE_HEIGHT - 1] = test_hash(333);

        let expected_root = compute_expected_root(transaction_nonce, &sibling_proofs);

        let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &expected_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_smt_exclusion_proof_wrong_root() {
        // Test with intentionally wrong root
        let transaction_nonce = 555u64;
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];
        let wrong_root = test_hash(999); // Arbitrary wrong root

        let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &wrong_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            PrivateChannelEscrowProgramError::InvalidSmtProof.into()
        );
    }

    #[test]
    fn test_verify_smt_exclusion_proof_corrupted_siblings() {
        // Test with siblings that don't match the expected proof
        let transaction_nonce = 777u64;
        let correct_siblings = [[0u8; 32]; TREE_HEIGHT];
        let expected_root = compute_expected_root(transaction_nonce, &correct_siblings);

        // Corrupt one sibling proof
        let mut corrupted_siblings = correct_siblings;
        corrupted_siblings[TREE_HEIGHT - 1] = test_hash(666);

        let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &expected_root,
            transaction_nonce,
            &corrupted_siblings,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_smt_exclusion_proof_edge_case_nonces() {
        // Test edge case nonce values
        let test_nonces = [0u64, 1u64, u64::MAX - 1, u64::MAX];

        for &nonce in &test_nonces {
            let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];
            let expected_root = compute_expected_root(nonce, &sibling_proofs);

            let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
                &expected_root,
                nonce,
                &sibling_proofs,
            );
            assert!(result.is_ok(), "Failed for nonce: {}", nonce);
        }
    }

    #[test]
    fn test_verify_smt_exclusion_proof_all_bits_set() {
        // Test with sibling proofs that have all bits set
        let transaction_nonce = 888u64;
        let sibling_proofs = [[0xFFu8; 32]; TREE_HEIGHT];
        let expected_root = compute_expected_root(transaction_nonce, &sibling_proofs);

        let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &expected_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_smt_exclusion_proof_early_termination() {
        // Test early termination when computed hash matches current root before final level
        let transaction_nonce = 123u64;
        let mut sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        // Set up siblings to cause early termination before the final level
        sibling_proofs[0] = test_hash(11);
        sibling_proofs[1] = test_hash(22);

        // Compute what the hash would be after the first two levels
        let leaf_position = transaction_nonce as usize % MAX_TREE_LEAVES;
        let mut current_hash = [0u8; 32];

        for (level, &sibling) in sibling_proofs.iter().enumerate().take(2) {
            let bit = (leaf_position >> level) & 1;
            current_hash = if bit == 0 {
                SparseMerkleTreeUtils::hash_combine(&current_hash, &sibling)
            } else {
                SparseMerkleTreeUtils::hash_combine(&sibling, &current_hash)
            };
        }

        // Use this intermediate hash as the "current root" to trigger early termination
        let result = SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &current_hash,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_smt_inclusion_proof_empty_tree() {
        // Test inclusion proof against tree with single transaction
        let transaction_nonce = 42u64;
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT]; // All empty siblings
        let expected_root = compute_expected_inclusion_root(transaction_nonce, &sibling_proofs);

        let result = SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &expected_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_smt_inclusion_proof_different_nonces() {
        // Test that different nonces produce different roots for inclusion
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        let nonce1 = 100u64;
        let nonce2 = 200u64;

        let root1 = compute_expected_inclusion_root(nonce1, &sibling_proofs);
        let root2 = compute_expected_inclusion_root(nonce2, &sibling_proofs);

        // Should be different unless hash collision (extremely unlikely)
        assert_ne!(root1, root2);

        // Each should verify against its own root
        assert!(
            SparseMerkleTreeUtils::verify_smt_inclusion_proof(&root1, nonce1, &sibling_proofs)
                .is_ok()
        );
        assert!(
            SparseMerkleTreeUtils::verify_smt_inclusion_proof(&root2, nonce2, &sibling_proofs)
                .is_ok()
        );

        // But not against the other's root
        assert!(
            SparseMerkleTreeUtils::verify_smt_inclusion_proof(&root1, nonce2, &sibling_proofs)
                .is_err()
        );
        assert!(
            SparseMerkleTreeUtils::verify_smt_inclusion_proof(&root2, nonce1, &sibling_proofs)
                .is_err()
        );
    }

    #[test]
    fn test_verify_smt_inclusion_proof_with_siblings() {
        // Test inclusion with non-empty sibling proofs
        let transaction_nonce = 1337u64;
        let mut sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        // Set some non-zero sibling values
        sibling_proofs[0] = test_hash(111);
        sibling_proofs[1] = test_hash(222);
        sibling_proofs[TREE_HEIGHT - 1] = test_hash(333);

        let expected_root = compute_expected_inclusion_root(transaction_nonce, &sibling_proofs);

        let result = SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &expected_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_smt_inclusion_proof_wrong_root() {
        // Test inclusion with intentionally wrong root
        let transaction_nonce = 555u64;
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];
        let wrong_root = test_hash(999); // Arbitrary wrong root

        let result = SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &wrong_root,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            PrivateChannelEscrowProgramError::InvalidSmtProof.into()
        );
    }

    #[test]
    fn test_verify_smt_inclusion_proof_corrupted_siblings() {
        // Test inclusion with siblings that don't match the expected proof
        let transaction_nonce = 777u64;
        let correct_siblings = [[0u8; 32]; TREE_HEIGHT];
        let expected_root = compute_expected_inclusion_root(transaction_nonce, &correct_siblings);

        // Corrupt one sibling proof
        let mut corrupted_siblings = correct_siblings;
        corrupted_siblings[TREE_HEIGHT - 1] = test_hash(666);

        let result = SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &expected_root,
            transaction_nonce,
            &corrupted_siblings,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_smt_inclusion_proof_edge_case_nonces() {
        // Test edge case nonce values for inclusion
        let test_nonces = [0u64, 1u64, u64::MAX - 1, u64::MAX];

        for &nonce in &test_nonces {
            let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];
            let expected_root = compute_expected_inclusion_root(nonce, &sibling_proofs);

            let result = SparseMerkleTreeUtils::verify_smt_inclusion_proof(
                &expected_root,
                nonce,
                &sibling_proofs,
            );
            assert!(result.is_ok(), "Failed inclusion for nonce: {}", nonce);
        }
    }

    #[test]
    fn test_verify_smt_inclusion_proof_early_termination() {
        // Test early termination for inclusion proof
        let transaction_nonce = 456u64;
        let mut sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        // Set up siblings to cause early termination before the final level
        sibling_proofs[0] = test_hash(77);
        sibling_proofs[1] = test_hash(88);

        // Compute what the hash would be after the first two levels
        let leaf_position = transaction_nonce as usize % MAX_TREE_LEAVES;
        let mut current_hash = NON_EMPTY_LEAF_HASH; // Start with actual leaf value for inclusion

        for (level, &sibling) in sibling_proofs.iter().enumerate().take(2) {
            let bit = (leaf_position >> level) & 1;
            current_hash = if bit == 0 {
                SparseMerkleTreeUtils::hash_combine(&current_hash, &sibling)
            } else {
                SparseMerkleTreeUtils::hash_combine(&sibling, &current_hash)
            };
        }

        // Use this intermediate hash as the "new root" to trigger early termination
        let result = SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &current_hash,
            transaction_nonce,
            &sibling_proofs,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_exclusion_vs_inclusion_proof_same_nonce() {
        // Test that exclusion and inclusion proofs for same nonce produce different roots
        let transaction_nonce = 1234u64;
        let sibling_proofs = [[0u8; 32]; TREE_HEIGHT];

        let exclusion_root = compute_expected_root(transaction_nonce, &sibling_proofs);
        let inclusion_root = compute_expected_inclusion_root(transaction_nonce, &sibling_proofs);

        // Should be different (exclusion starts with empty, inclusion starts with leaf_key)
        assert_ne!(exclusion_root, inclusion_root);

        // Exclusion proof should work with exclusion root
        assert!(SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &exclusion_root,
            transaction_nonce,
            &sibling_proofs
        )
        .is_ok());

        // Inclusion proof should work with inclusion root
        assert!(SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &inclusion_root,
            transaction_nonce,
            &sibling_proofs
        )
        .is_ok());

        // Cross-validation should fail
        assert!(SparseMerkleTreeUtils::verify_smt_exclusion_proof(
            &inclusion_root,
            transaction_nonce,
            &sibling_proofs
        )
        .is_err());

        assert!(SparseMerkleTreeUtils::verify_smt_inclusion_proof(
            &exclusion_root,
            transaction_nonce,
            &sibling_proofs
        )
        .is_err());
    }
}
