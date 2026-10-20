import { expect } from '@jest/globals';
import { getAllowedWalletEncoder } from '../../../src/generated';

describe('AllowedWallet Account', () => {
    // Opposite flags pin the field order to the program's layout.
    it('should encode discriminator, bump, depositsBlocked, withdrawalsBlocked in 4 bytes', () => {
        const encoder = getAllowedWalletEncoder();
        const encodedData = encoder.encode({
            discriminator: 4,
            bump: 254,
            depositsBlocked: true,
            withdrawalsBlocked: false,
        });

        expect(encoder.fixedSize).toBe(4);
        expect(Array.from(encodedData)).toEqual([4, 254, 1, 0]);
    });
});
