import { expect } from '@jest/globals';
import { findAllowedWalletPda } from '../../../src/generated/pdas/allowedWallet';
import { TEST_ADDRESSES } from '../../setup/mocks';
import { expectedAllowedWalletPda } from './pda-helpers';

describe('AllowedWallet PDA', () => {
    it('should derive from ["allowed_wallet", instance, wallet]', async () => {
        const generatedPda = await findAllowedWalletPda({
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
        });
        const expectedPda = await expectedAllowedWalletPda(TEST_ADDRESSES.INSTANCE, TEST_ADDRESSES.WALLET);

        expect(generatedPda).toEqual(expectedPda);
    });
});
