import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    getMoveToConfidentialInstruction,
    getMoveToConfidentialInstructionDataCodec,
    MOVE_TO_CONFIDENTIAL_DISCRIMINATOR,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('moveToConfidential', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);
    const amount = BigInt(1_234_567);

    const instruction = getMoveToConfidentialInstruction({
        payer,
        operator,
        instance: TEST_ADDRESSES.INSTANCE,
        operatorPda: TEST_ADDRESSES.INSTANCE_SEED,
        mint: TEST_ADDRESSES.MINT,
        instanceAta: TEST_ADDRESSES.INSTANCE_ATA,
        amount,
    });

    it('should round-trip amount', () => {
        expect(MOVE_TO_CONFIDENTIAL_DISCRIMINATOR).toBe(16);
        expect(getMoveToConfidentialInstructionDataCodec().decode(instruction.data)).toEqual({
            discriminator: 16,
            amount,
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
