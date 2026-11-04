import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    APPLY_PENDING_BALANCE_DISCRIMINATOR,
    getApplyPendingBalanceInstruction,
    getApplyPendingBalanceInstructionDataCodec,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('applyPendingBalance', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);
    const args = {
        expectedCreditCounter: BigInt(12_345),
        newDecryptableAvailableBalance: Array(36).fill(1),
    };

    const instruction = getApplyPendingBalanceInstruction({
        payer,
        operator,
        instance: TEST_ADDRESSES.INSTANCE,
        operatorPda: TEST_ADDRESSES.INSTANCE_SEED,
        mint: TEST_ADDRESSES.MINT,
        instanceAta: TEST_ADDRESSES.INSTANCE_ATA,
        ...args,
    });

    it('should round-trip expectedCreditCounter and newDecryptableAvailableBalance', () => {
        expect(APPLY_PENDING_BALANCE_DISCRIMINATOR).toBe(15);
        expect(getApplyPendingBalanceInstructionDataCodec().decode(instruction.data)).toEqual({
            discriminator: 15,
            ...args,
        });
    });

    it('should mark payer and operator as signers and the escrow account writable', () => {
        expect(instruction.accounts.map(account => account.role)).toEqual([
            AccountRole.WRITABLE_SIGNER, // payer
            AccountRole.READONLY_SIGNER, // operator
            AccountRole.READONLY, // instance
            AccountRole.READONLY, // operatorPda
            AccountRole.READONLY, // mint
            AccountRole.WRITABLE, // instanceAta
            AccountRole.READONLY, // tokenProgram
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
        ]);
    });
});
