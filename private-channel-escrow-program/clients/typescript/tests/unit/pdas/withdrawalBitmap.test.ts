import { expect } from '@jest/globals';
import { address, assertIsAddress } from '@solana/kit';
import { findWithdrawalBitmapPda, type WithdrawalBitmapSeeds } from '../../../src/generated/pdas/withdrawalBitmap';
import { expectedWithdrawalBitmapPda } from './pda-helpers';
import { TEST_ADDRESSES } from '../../setup/mocks';

describe('WithdrawalBitmap PDA', () => {
    const sampleInstance1 = TEST_ADDRESSES.INSTANCE_SEED;
    const sampleInstance2 = address('8cPFGPZbUE7DQPrw24GgTYNkvr2FLnHfgqgjCxEn73K6');

    // Pins the seed string and ordering against an independent derivation. They
    // must match the on-chain WITHDRAWAL_BITMAP_SEED or every lookup misses.
    it('should generate withdrawal bitmap PDA matching expected values', async () => {
        const seeds: WithdrawalBitmapSeeds = {
            instance: sampleInstance1,
        };

        const generatedPda = await findWithdrawalBitmapPda(seeds);
        const expectedPda = await expectedWithdrawalBitmapPda(sampleInstance1);

        expect(generatedPda[0]).toBe(expectedPda[0]); // address
        expect(generatedPda[1]).toBe(expectedPda[1]); // bump
        assertIsAddress(generatedPda[0]);
    });

    // Each instance owns exactly one bitmap. Dropping the instance seed would
    // make every instance share one, so any could consume another's nonces.
    it('should generate different bitmap PDAs for different instances', async () => {
        const pda1 = await findWithdrawalBitmapPda({ instance: sampleInstance1 });
        const pda2 = await findWithdrawalBitmapPda({ instance: sampleInstance2 });

        expect(pda1[0]).not.toBe(pda2[0]);
    });
});
