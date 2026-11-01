import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    CONFIDENTIAL_RELEASE_FUNDS_DISCRIMINATOR,
    getConfidentialReleaseFundsInstruction,
    getConfidentialReleaseFundsInstructionDataCodec,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, TEST_TRANSACTION_NONCE } from '../../setup/mocks';

describe('confidentialReleaseFunds', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);
    const args = {
        user: TEST_ADDRESSES.WALLET,
        transactionNonce: BigInt(TEST_TRANSACTION_NONCE),
        withdrawer: TEST_ADDRESSES.ADMIN,
        newSourceDecryptableAvailableBalance: Array(36).fill(1),
        auditorCiphertextLo: Array(64).fill(2),
        auditorCiphertextHi: Array(64).fill(3),
        equalityProofInstructionOffset: 1,
        ciphertextValidityProofInstructionOffset: 2,
        rangeProofInstructionOffset: 5,
        feeSigmaProofInstructionOffset: 3,
        feeCiphertextValidityProofInstructionOffset: 4,
    };

    const instruction = getConfidentialReleaseFundsInstruction({
        payer,
        operator,
        instance: TEST_ADDRESSES.INSTANCE,
        withdrawalBitmap: TEST_ADDRESSES.INSTANCE_SEED,
        operatorPda: TEST_ADDRESSES.INSTANCE_SEED_2,
        mint: TEST_ADDRESSES.MINT,
        allowedMint: TEST_ADDRESSES.ALLOWED_MINT,
        userAta: TEST_ADDRESSES.WALLET,
        instanceAta: TEST_ADDRESSES.INSTANCE_ATA,
        userAllowedWalletPda: TEST_ADDRESSES.ADMIN,
        withdrawerAllowedWalletPda: TEST_ADDRESSES.ADMIN,
        ...args,
    });

    it('should round-trip user, nonce, withdrawer and the transfer fields', () => {
        expect(CONFIDENTIAL_RELEASE_FUNDS_DISCRIMINATOR).toBe(14);
        expect(getConfidentialReleaseFundsInstructionDataCodec().decode(instruction.data)).toEqual({
            discriminator: 14,
            ...args,
        });
    });

    it('should mark payer and operator as signers and bitmap and both token accounts writable', () => {
        expect(instruction.accounts.map(account => account.role)).toEqual([
            AccountRole.WRITABLE_SIGNER, // payer
            AccountRole.READONLY_SIGNER, // operator
            AccountRole.READONLY, // instance
            AccountRole.WRITABLE, // withdrawalBitmap
            AccountRole.READONLY, // operatorPda
            AccountRole.READONLY, // mint
            AccountRole.READONLY, // allowedMint
            AccountRole.WRITABLE, // userAta
            AccountRole.WRITABLE, // instanceAta
            AccountRole.READONLY, // tokenProgram
            AccountRole.READONLY, // instructionsSysvar
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
            AccountRole.READONLY, // memoProgram
            AccountRole.READONLY, // userAllowedWalletPda
            AccountRole.READONLY, // withdrawerAllowedWalletPda
        ]);
    });
});
