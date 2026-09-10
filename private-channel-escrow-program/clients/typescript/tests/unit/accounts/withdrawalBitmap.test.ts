import { expect } from '@jest/globals';
import {
    getWithdrawalBitmapEncoder,
    getWithdrawalBitmapDecoder,
    getWithdrawalBitmapCodec,
    type WithdrawalBitmap,
} from '../../../src/generated';

// Only the fixed header is described by the codec. The 8192 bytes of nonce bits
// follow it on-chain and are read by slicing, so they are not part of this size.
const EXPECTED_HEADER_SIZE =
    1 + // discriminator
    1 + // bump
    8; // generation

describe('WithdrawalBitmap Account', () => {
    describe('Encoder/Decoder functionality', () => {
        it('should encode and decode header data correctly', () => {
            const testBitmap: WithdrawalBitmap = {
                discriminator: 3,
                bump: 254,
                generation: 0n,
            };

            const encoder = getWithdrawalBitmapEncoder();
            const decoder = getWithdrawalBitmapDecoder();
            const decodedBitmap = decoder.decode(encoder.encode(testBitmap));

            expect(decodedBitmap.discriminator).toBe(testBitmap.discriminator);
            expect(decodedBitmap.bump).toBe(testBitmap.bump);
            expect(decodedBitmap.generation).toBe(testBitmap.generation);
        });

        it('should handle combined codec correctly', () => {
            const testBitmap: WithdrawalBitmap = {
                discriminator: 3,
                bump: 255,
                generation: 42n,
            };

            const codec = getWithdrawalBitmapCodec();
            const decodedBitmap = codec.decode(codec.encode(testBitmap));

            expect(decodedBitmap).toEqual(testBitmap);
        });

        it('should handle different generation values (u64)', () => {
            const testGenerations = [0n, 1n, 65535n, 4294967296n, 18446744073709551615n];

            for (const generation of testGenerations) {
                const testBitmap: WithdrawalBitmap = {
                    discriminator: 3,
                    bump: 254,
                    generation,
                };

                const codec = getWithdrawalBitmapCodec();
                const decodedBitmap = codec.decode(codec.encode(testBitmap));

                expect(decodedBitmap.generation).toBe(generation);
                expect(typeof decodedBitmap.generation).toBe('bigint');
            }
        });

        // The account is 8202 bytes on-chain but the codec only describes the
        // header, so decoding must ignore the trailing bits rather than throw.
        it('should decode a full-length account and ignore the trailing bits', () => {
            const testBitmap: WithdrawalBitmap = {
                discriminator: 3,
                bump: 254,
                generation: 7n,
            };

            const header = getWithdrawalBitmapEncoder().encode(testBitmap);
            const fullAccount = new Uint8Array(EXPECTED_HEADER_SIZE + 8192);
            fullAccount.set(header, 0);
            // Set some nonce bits so the tail is not all zeroes.
            fullAccount[EXPECTED_HEADER_SIZE] = 0b1010_1010;

            const decodedBitmap = getWithdrawalBitmapDecoder().decode(fullAccount);

            expect(decodedBitmap.bump).toBe(254);
            expect(decodedBitmap.generation).toBe(7n);
        });
    });

    describe('Size validation', () => {
        it('should report the header size (10 bytes)', () => {
            expect(getWithdrawalBitmapEncoder().fixedSize).toBe(EXPECTED_HEADER_SIZE);
        });
    });
});
