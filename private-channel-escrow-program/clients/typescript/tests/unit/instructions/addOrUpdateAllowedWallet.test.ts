import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    getAddOrUpdateAllowedWalletInstructionAsync,
    findAllowedWalletPda,
    ADD_OR_UPDATE_ALLOWED_WALLET_DISCRIMINATOR,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('addOrUpdateAllowedWallet', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

    // The guide's example passes neither, so both defaults must match the program.
    it('should derive allowedWalletPda and bump from instance and wallet', async () => {
        const [expectedPda, expectedBump] = await findAllowedWalletPda({
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
        });

        const instruction = await getAddOrUpdateAllowedWalletInstructionAsync({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
            blockDeposits: false,
            blockWithdrawals: false,
        });

        expect(instruction.accounts[4].address).toBe(expectedPda);
        expect(instruction.data[1]).toBe(expectedBump);
    });

    // Opposite flags pin the byte order the program reads.
    it('should encode discriminator, bump, blockDeposits, blockWithdrawals', async () => {
        const instruction = await getAddOrUpdateAllowedWalletInstructionAsync({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
            bump: 254,
            blockDeposits: true,
            blockWithdrawals: false,
        });

        expect(ADD_OR_UPDATE_ALLOWED_WALLET_DISCRIMINATOR).toBe(9);
        expect(Array.from(instruction.data)).toEqual([9, 254, 1, 0]);
    });

    it('should mark payer and admin as signers and only allowedWalletPda writable besides payer', async () => {
        const instruction = await getAddOrUpdateAllowedWalletInstructionAsync({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
            blockDeposits: false,
            blockWithdrawals: false,
        });

        expect(instruction.accounts.map(account => account.role)).toEqual([
            AccountRole.WRITABLE_SIGNER, // payer
            AccountRole.READONLY_SIGNER, // admin
            AccountRole.READONLY, // instance
            AccountRole.READONLY, // wallet
            AccountRole.WRITABLE, // allowedWalletPda
            AccountRole.READONLY, // systemProgram
            AccountRole.READONLY, // eventAuthority
            AccountRole.READONLY, // privateChannelEscrowProgram
        ]);
    });
});
