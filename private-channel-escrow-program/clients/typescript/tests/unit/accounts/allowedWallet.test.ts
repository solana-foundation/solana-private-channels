import { expect } from '@jest/globals';
import { getAllowedWalletEncoder } from '../../../src/generated';

describe('AllowedWallet Account', () => {
    // Opposite flags pin the field order to the program's layout.
    it('should encode discriminator, bump, depositsBlocked, withdrawalsBlocked, then 32 reserved bytes', () => {
        const reserved = new Array<number>(32).fill(0);
        const encoder = getAllowedWalletEncoder();
        const encodedData = encoder.encode({
            discriminator: 4,
            bump: 254,
            depositsBlocked: true,
            withdrawalsBlocked: false,
            reserved,
        });

        expect(encoder.fixedSize).toBe(36);
        expect(Array.from(encodedData)).toEqual([4, 254, 1, 0, ...reserved]);
    });
});
