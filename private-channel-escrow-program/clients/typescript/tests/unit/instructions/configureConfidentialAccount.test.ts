import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    CONFIGURE_CONFIDENTIAL_ACCOUNT_DISCRIMINATOR,
    getConfigureConfidentialAccountInstruction,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('configureConfidentialAccount', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
    const decryptableZeroBalance = Array(36).fill(7);

    const instruction = getConfigureConfidentialAccountInstruction({
        payer,
        admin,
        instance: TEST_ADDRESSES.INSTANCE,
        mint: TEST_ADDRESSES.MINT,
        allowedMint: TEST_ADDRESSES.ALLOWED_MINT,
        instanceAta: TEST_ADDRESSES.INSTANCE_ATA,
        decryptableZeroBalance,
        proofInstructionOffset: -1,
    });

    it('should encode discriminator, decryptableZeroBalance and a signed proofInstructionOffset', () => {
        expect(CONFIGURE_CONFIDENTIAL_ACCOUNT_DISCRIMINATOR).toBe(12);
        expect(Array.from(instruction.data)).toEqual([12, ...decryptableZeroBalance, 255]);
    });

    it('should mark payer and admin as signers and the escrow account writable', () => {
        expect(instruction.accounts.map(account => account.role)).toEqual([
            AccountRole.WRITABLE_SIGNER, // payer
            AccountRole.READONLY_SIGNER, // admin
            AccountRole.READONLY, // instance
            AccountRole.READONLY, // mint
            AccountRole.READONLY, // allowedMint
            AccountRole.WRITABLE, // instanceAta
            AccountRole.READONLY, // systemProgram
            AccountRole.READONLY, // tokenProgram
            AccountRole.READONLY, // instructionsSysvar
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
        ]);
    });
});
