import { expect } from '@jest/globals';
import { AccountRole, none, some } from '@solana/kit';
import { TOKEN_2022_PROGRAM_ADDRESS } from '@solana-program/token-2022';
import {
    CONFIDENTIAL_DEPOSIT_DISCRIMINATOR,
    getConfidentialDepositInstruction,
    getConfidentialDepositInstructionDataCodec,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('confidentialDeposit', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
    const transfer = {
        newSourceDecryptableAvailableBalance: Array(36).fill(1),
        auditorCiphertextLo: Array(64).fill(2),
        auditorCiphertextHi: Array(64).fill(3),
        equalityProofInstructionOffset: 1,
        ciphertextValidityProofInstructionOffset: 2,
        rangeProofInstructionOffset: 3,
        feeSigmaProofInstructionOffset: 0,
        feeCiphertextValidityProofInstructionOffset: 0,
    };
    const accounts = {
        payer,
        user,
        instance: TEST_ADDRESSES.INSTANCE,
        mint: TEST_ADDRESSES.MINT,
        allowedMint: TEST_ADDRESSES.ALLOWED_MINT,
        userAta: TEST_ADDRESSES.WALLET,
        instanceAta: TEST_ADDRESSES.INSTANCE_ATA,
        userAllowedWalletPda: TEST_ADDRESSES.ADMIN,
        recipientAllowedWalletPda: TEST_ADDRESSES.ADMIN,
    };

    it('should round-trip the transfer fields with and without a recipient', () => {
        const codec = getConfidentialDepositInstructionDataCodec();

        const withRecipient = getConfidentialDepositInstruction({
            ...accounts,
            ...transfer,
            recipient: TEST_ADDRESSES.OPERATOR,
        });
        const withoutRecipient = getConfidentialDepositInstruction({ ...accounts, ...transfer, recipient: null });

        expect(CONFIDENTIAL_DEPOSIT_DISCRIMINATOR).toBe(13);
        expect(codec.decode(withRecipient.data)).toEqual({
            discriminator: 13,
            ...transfer,
            recipient: some(TEST_ADDRESSES.OPERATOR),
        });
        expect(codec.decode(withoutRecipient.data).recipient).toEqual(none());
    });

    it('should default to Token-2022 and the instructions sysvar', () => {
        const instruction = getConfidentialDepositInstruction({ ...accounts, ...transfer, recipient: null });

        expect(instruction.accounts[7].address).toBe(TOKEN_2022_PROGRAM_ADDRESS);
        expect(instruction.accounts[8].address).toBe('Sysvar1nstructions1111111111111111111111111');
        expect(instruction.accounts.map(account => account.role)).toEqual([
            AccountRole.WRITABLE_SIGNER, // payer
            AccountRole.READONLY_SIGNER, // user
            AccountRole.READONLY, // instance
            AccountRole.READONLY, // mint
            AccountRole.READONLY, // allowedMint
            AccountRole.WRITABLE, // userAta
            AccountRole.WRITABLE, // instanceAta
            AccountRole.READONLY, // tokenProgram
            AccountRole.READONLY, // instructionsSysvar
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
            AccountRole.READONLY, // userAllowedWalletPda
            AccountRole.READONLY, // recipientAllowedWalletPda
        ]);
    });
});
