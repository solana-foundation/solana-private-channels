import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    getRemoveAllowedWalletInstructionAsync,
    findAllowedWalletPda,
    REMOVE_ALLOWED_WALLET_DISCRIMINATOR,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';

describe('removeAllowedWallet', () => {
    const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
    const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

    it('should derive allowedWalletPda from instance and wallet', async () => {
        const [expectedPda] = await findAllowedWalletPda({
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
        });

        const instruction = await getRemoveAllowedWalletInstructionAsync({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
        });

        expect(instruction.accounts[4].address).toBe(expectedPda);
    });

    it('should encode only the discriminator', async () => {
        const instruction = await getRemoveAllowedWalletInstructionAsync({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
        });

        expect(REMOVE_ALLOWED_WALLET_DISCRIMINATOR).toBe(10);
        expect(Array.from(instruction.data)).toEqual([10]);
    });

    it('should mark payer and admin as signers and only allowedWalletPda writable besides payer', async () => {
        const instruction = await getRemoveAllowedWalletInstructionAsync({
            payer,
            admin,
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
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
