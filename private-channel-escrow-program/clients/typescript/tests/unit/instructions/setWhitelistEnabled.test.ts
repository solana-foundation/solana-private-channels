import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import { getSetWhitelistEnabledInstruction, SET_WHITELIST_ENABLED_DISCRIMINATOR } from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('setWhitelistEnabled', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

    // Opposite flags pin the byte order the program reads.
    it('should encode discriminator, whitelistDeposits, whitelistWithdrawals', () => {
        const instruction = getSetWhitelistEnabledInstruction({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            whitelistDeposits: true,
            whitelistWithdrawals: false,
        });

        expect(SET_WHITELIST_ENABLED_DISCRIMINATOR).toBe(11);
        expect(Array.from(instruction.data)).toEqual([11, 1, 0]);
    });

    it('should mark payer and admin as signers and instance writable', () => {
        const instruction = getSetWhitelistEnabledInstruction({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            whitelistDeposits: true,
            whitelistWithdrawals: false,
        });

        expect(instruction.accounts.map(account => account.role)).toEqual([
            AccountRole.WRITABLE_SIGNER, // payer
            AccountRole.READONLY_SIGNER, // admin
            AccountRole.WRITABLE, // instance
            AccountRole.READONLY, // systemProgram
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
        ]);
    });
});
