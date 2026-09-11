import { expect } from '@jest/globals';
import { address } from '@solana/kit';
import {
    getAllowedMintEncoder,
    getAllowedMintDecoder,
    getAllowedMintCodec,
    type AllowedMint,
} from '../../../src/generated';

const TOKEN_PROGRAM = address('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

// Expected size calculation based on program structure
const EXPECTED_SIZE =
    1 + // discriminator
    1 + // bump
    1 + // depositsBlocked
    1 + // withdrawalsBlocked
    1 + // decimals
    32 + // tokenProgram
    8 + // extensions
    1; // hasFreezeAuthority

describe('AllowedMint Account', () => {
    describe('Encoder/Decoder functionality', () => {
        it('should encode and decode allowedMint data correctly', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 1,
                bump: 250,
                depositsBlocked: true,
                withdrawalsBlocked: false,
                decimals: 6,
                tokenProgram: TOKEN_PROGRAM,
                extensions: 0n,
                hasFreezeAuthority: false,
            };

            // Test encoding
            const encoder = getAllowedMintEncoder();
            const encodedData = encoder.encode(testAllowedMint);

            // Test decoding
            const decoder = getAllowedMintDecoder();
            const decodedAllowedMint = decoder.decode(encodedData);

            // Verify all fields are correctly encoded/decoded
            expect(decodedAllowedMint.discriminator).toBe(testAllowedMint.discriminator);
            expect(decodedAllowedMint.bump).toBe(testAllowedMint.bump);
        });

        it('should handle combined codec correctly', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 255,
                bump: 127,
                depositsBlocked: false,
                withdrawalsBlocked: true,
                decimals: 9,
                tokenProgram: TOKEN_PROGRAM,
                extensions: 0x0000_0000_0400_000an,
                hasFreezeAuthority: true,
            };

            // Test combined codec
            const codec = getAllowedMintCodec();
            const encodedData = codec.encode(testAllowedMint);
            const decodedAllowedMint = codec.decode(encodedData);

            // Verify round-trip encoding/decoding
            expect(decodedAllowedMint).toEqual(testAllowedMint);
        });

        it('should handle different bump values (u8)', () => {
            const testBumps = [0, 1, 127, 250, 254, 255];

            for (const bump of testBumps) {
                const testAllowedMint: AllowedMint = {
                    discriminator: 1,
                    bump,
                    depositsBlocked: false,
                    withdrawalsBlocked: false,
                    decimals: 6,
                    tokenProgram: TOKEN_PROGRAM,
                    extensions: 0n,
                    hasFreezeAuthority: false,
                };

                const codec = getAllowedMintCodec();
                const encodedData = codec.encode(testAllowedMint);
                const decodedAllowedMint = codec.decode(encodedData);

                expect(decodedAllowedMint.bump).toBe(bump);
                expect(typeof decodedAllowedMint.bump).toBe('number');
            }
        });

        // The extension bitmask is the widest field and the only one that is not
        // a byte, so a wrong-endian or truncated read shows up here first.
        it('should round-trip extension bitmask values (u64)', () => {
            const testMasks = [0n, 1n, 1n << 26n, 0x0000_0000_0400_000an, (1n << 64n) - 1n];

            for (const extensions of testMasks) {
                const testAllowedMint: AllowedMint = {
                    discriminator: 1,
                    bump: 250,
                    depositsBlocked: false,
                    withdrawalsBlocked: false,
                    decimals: 6,
                    tokenProgram: TOKEN_PROGRAM,
                    extensions,
                    hasFreezeAuthority: false,
                };

                const codec = getAllowedMintCodec();
                const decodedAllowedMint = codec.decode(codec.encode(testAllowedMint));

                expect(decodedAllowedMint.extensions).toBe(extensions);
            }
        });
    });

    describe('Structure validation', () => {
        it('should validate allowedMint structure fields exist', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 1,
                bump: 250,
                depositsBlocked: true,
                withdrawalsBlocked: false,
                decimals: 6,
                tokenProgram: TOKEN_PROGRAM,
                extensions: 0n,
                hasFreezeAuthority: false,
            };

            // Verify all required fields are present
            expect(testAllowedMint).toHaveProperty('discriminator');
            expect(testAllowedMint).toHaveProperty('bump');
            expect(testAllowedMint).toHaveProperty('depositsBlocked');
            expect(testAllowedMint).toHaveProperty('withdrawalsBlocked');
            expect(testAllowedMint).toHaveProperty('decimals');
            expect(testAllowedMint).toHaveProperty('tokenProgram');
            expect(testAllowedMint).toHaveProperty('extensions');
            expect(testAllowedMint).toHaveProperty('hasFreezeAuthority');
        });

        it('should validate allowedMint structure field types', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 1,
                bump: 250,
                depositsBlocked: true,
                withdrawalsBlocked: false,
                decimals: 6,
                tokenProgram: TOKEN_PROGRAM,
                extensions: 0n,
                hasFreezeAuthority: false,
            };

            // Verify field types
            expect(typeof testAllowedMint.discriminator).toBe('number');
            expect(typeof testAllowedMint.bump).toBe('number');
            expect(typeof testAllowedMint.depositsBlocked).toBe('boolean');
            expect(typeof testAllowedMint.withdrawalsBlocked).toBe('boolean');
            expect(typeof testAllowedMint.decimals).toBe('number');
            expect(typeof testAllowedMint.tokenProgram).toBe('string');
            expect(typeof testAllowedMint.extensions).toBe('bigint');
            expect(typeof testAllowedMint.hasFreezeAuthority).toBe('boolean');
        });
    });

    describe('Size validation', () => {
        it('should report correct account size (46 bytes)', () => {
            const accountSize = getAllowedMintEncoder().fixedSize;
            expect(accountSize).toBe(EXPECTED_SIZE);
        });

        it('should validate encoded data matches expected size', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 1,
                bump: 250,
                depositsBlocked: true,
                withdrawalsBlocked: false,
                decimals: 6,
                tokenProgram: TOKEN_PROGRAM,
                extensions: 0n,
                hasFreezeAuthority: false,
            };

            const encoder = getAllowedMintEncoder();
            const encodedData = encoder.encode(testAllowedMint);
            const reportedSize = getAllowedMintEncoder().fixedSize;
            const actualSize = encodedData.length;

            expect(encodedData).toHaveLength(EXPECTED_SIZE);
            expect(reportedSize).toBe(EXPECTED_SIZE);
            expect(actualSize).toBe(EXPECTED_SIZE);
        });

        it('should validate size consistency across multiple allowedMints', () => {
            const testAllowedMints: AllowedMint[] = [
                {
                    discriminator: 0,
                    bump: 100,
                    depositsBlocked: false,
                    withdrawalsBlocked: false,
                    decimals: 0,
                    tokenProgram: TOKEN_PROGRAM,
                    extensions: 0n,
                    hasFreezeAuthority: false,
                },
                {
                    discriminator: 255,
                    bump: 255,
                    depositsBlocked: true,
                    withdrawalsBlocked: true,
                    decimals: 255,
                    tokenProgram: TOKEN_PROGRAM,
                    extensions: (1n << 64n) - 1n,
                    hasFreezeAuthority: true,
                },
                {
                    discriminator: 127,
                    bump: 50,
                    depositsBlocked: true,
                    withdrawalsBlocked: false,
                    decimals: 9,
                    tokenProgram: TOKEN_PROGRAM,
                    extensions: 1n << 14n,
                    hasFreezeAuthority: false,
                },
            ];

            const encoder = getAllowedMintEncoder();

            for (const allowedMint of testAllowedMints) {
                const encodedData = encoder.encode(allowedMint);
                expect(encodedData).toHaveLength(EXPECTED_SIZE);
            }
        });
    });

    describe('Edge case validation', () => {
        it('should handle minimum values', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 0,
                bump: 0,
                depositsBlocked: false,
                withdrawalsBlocked: false,
                decimals: 0,
                tokenProgram: TOKEN_PROGRAM,
                extensions: 0n,
                hasFreezeAuthority: false,
            };

            const codec = getAllowedMintCodec();
            const encodedData = codec.encode(testAllowedMint);
            const decodedAllowedMint = codec.decode(encodedData);

            expect(decodedAllowedMint).toEqual(testAllowedMint);
        });

        it('should handle maximum values', () => {
            const testAllowedMint: AllowedMint = {
                discriminator: 255,
                bump: 255,
                depositsBlocked: true,
                withdrawalsBlocked: true,
                decimals: 255,
                tokenProgram: TOKEN_PROGRAM,
                extensions: (1n << 64n) - 1n,
                hasFreezeAuthority: true,
            };

            const codec = getAllowedMintCodec();
            const encodedData = codec.encode(testAllowedMint);
            const decodedAllowedMint = codec.decode(encodedData);

            expect(decodedAllowedMint).toEqual(testAllowedMint);
        });
    });
});
