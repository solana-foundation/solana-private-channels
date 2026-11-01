import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    getMoveToPublicInstruction,
    getMoveToPublicInstructionDataCodec,
    MOVE_TO_PUBLIC_DISCRIMINATOR,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('moveToPublic', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);
    const args = {
        amount: BigInt(1_234_567),
        newDecryptableAvailableBalance: Array(36).fill(1),
        equalityProofInstructionOffset: 1,
        rangeProofInstructionOffset: 0,
    };

    const instruction = getMoveToPublicInstruction({
        payer,
        operator,
        instance: TEST_ADDRESSES.INSTANCE,
        operatorPda: TEST_ADDRESSES.INSTANCE_SEED,
        mint: TEST_ADDRESSES.MINT,
        instanceAta: TEST_ADDRESSES.INSTANCE_ATA,
        ...args,
    });

    it('should round-trip amount, newDecryptableAvailableBalance and the proof offsets', () => {
        expect(MOVE_TO_PUBLIC_DISCRIMINATOR).toBe(17);
        expect(getMoveToPublicInstructionDataCodec().decode(instruction.data)).toEqual({
            discriminator: 17,
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
            AccountRole.READONLY, // instructionsSysvar
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
        ]);
    });
});
