import { expect } from '@jest/globals';
import { getInstanceEncoder, getInstanceDecoder, getInstanceCodec, type Instance } from '../../../src/generated';
import { TEST_ADDRESSES } from '../../setup/mocks';
import { assertIsAddress, type Address } from '@solana/kit';

const EXPECTED_SIZE =
    1 + // discriminator
    1 + // bump
    1 + // version
    32 + // instance_seed
    32; // admin

describe('Instance Account', () => {
    describe('Encoder/Decoder functionality', () => {
        it('should encode and decode instance data correctly', () => {
            const testInstance: Instance = {
                discriminator: 0,
                bump: 250,
                version: 1,
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                admin: TEST_ADDRESSES.ADMIN,
            };

            // Test encoding
            const encoder = getInstanceEncoder();
            const encodedData = encoder.encode(testInstance);

            // Test decoding
            const decoder = getInstanceDecoder();
            const decodedInstance = decoder.decode(encodedData);

            // Verify all fields are correctly encoded/decoded
            expect(decodedInstance.discriminator).toBe(testInstance.discriminator);
            expect(decodedInstance.bump).toBe(testInstance.bump);
            expect(decodedInstance.version).toBe(testInstance.version);
            expect(decodedInstance.instanceSeed).toBe(testInstance.instanceSeed);
            expect(decodedInstance.admin).toBe(testInstance.admin);
        });

        it('should handle combined codec correctly', () => {
            const testInstance: Instance = {
                discriminator: 1,
                bump: 255,
                version: 2,
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED_2,
                admin: TEST_ADDRESSES.WALLET,
            };

            // Test combined codec
            const codec = getInstanceCodec();
            const encodedData = codec.encode(testInstance);
            const decodedInstance = codec.decode(encodedData);

            // Verify round-trip encoding/decoding
            expect(decodedInstance).toEqual(testInstance);
        });

        it('should handle different bump values (u8)', () => {
            const testBumps = [0, 1, 127, 250, 254, 255];

            for (const bump of testBumps) {
                const testInstance: Instance = {
                    discriminator: 0,
                    bump,
                    version: 1,
                    instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                    admin: TEST_ADDRESSES.ADMIN,
                };

                const codec = getInstanceCodec();
                const encodedData = codec.encode(testInstance);
                const decodedInstance = codec.decode(encodedData);

                expect(decodedInstance.bump).toBe(bump);
                expect(typeof decodedInstance.bump).toBe('number');
            }
        });

        it('should handle different version values (u8)', () => {
            const testVersions = [0, 1, 2, 10, 100, 255];

            for (const version of testVersions) {
                const testInstance: Instance = {
                    discriminator: 0,
                    bump: 250,
                    version,
                    instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                    admin: TEST_ADDRESSES.ADMIN,
                };

                const codec = getInstanceCodec();
                const encodedData = codec.encode(testInstance);
                const decodedInstance = codec.decode(encodedData);

                expect(decodedInstance.version).toBe(version);
                expect(typeof decodedInstance.version).toBe('number');
            }
        });

        it('should handle different address values correctly', () => {
            const testAddresses = [
                { instanceSeed: TEST_ADDRESSES.INSTANCE_SEED, admin: TEST_ADDRESSES.ADMIN },
                { instanceSeed: TEST_ADDRESSES.INSTANCE_SEED_2, admin: TEST_ADDRESSES.WALLET },
                { instanceSeed: TEST_ADDRESSES.USDC_MINT, admin: TEST_ADDRESSES.OPERATOR },
                { instanceSeed: TEST_ADDRESSES.WRAPPED_SOL, admin: TEST_ADDRESSES.PAYER },
            ];

            for (const addresses of testAddresses) {
                const testInstance: Instance = {
                    discriminator: 0,
                    bump: 250,
                    version: 1,
                    instanceSeed: addresses.instanceSeed as Address,
                    admin: addresses.admin as Address,
                };

                const codec = getInstanceCodec();
                const encodedData = codec.encode(testInstance);
                const decodedInstance = codec.decode(encodedData);

                expect(decodedInstance.instanceSeed).toBe(addresses.instanceSeed);
                expect(decodedInstance.admin).toBe(addresses.admin);
                assertIsAddress(decodedInstance.instanceSeed);
                assertIsAddress(decodedInstance.admin);
            }
        });
    });

    describe('Structure validation', () => {
        it('should validate instance structure fields exist', () => {
            const testInstance: Instance = {
                discriminator: 0,
                bump: 250,
                version: 1,
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                admin: TEST_ADDRESSES.ADMIN,
            };

            // Verify all required fields are present
            expect(testInstance).toHaveProperty('discriminator');
            expect(testInstance).toHaveProperty('bump');
            expect(testInstance).toHaveProperty('version');
            expect(testInstance).toHaveProperty('instanceSeed');
            expect(testInstance).toHaveProperty('admin');
        });

        it('should validate instance structure field types', () => {
            const testInstance: Instance = {
                discriminator: 0,
                bump: 250,
                version: 1,
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                admin: TEST_ADDRESSES.ADMIN,
            };

            // Verify field types
            expect(typeof testInstance.discriminator).toBe('number');
            expect(typeof testInstance.bump).toBe('number');
            expect(typeof testInstance.version).toBe('number');
            expect(typeof testInstance.instanceSeed).toBe('string');
            expect(typeof testInstance.admin).toBe('string');
        });

        // Replay state moved to the WithdrawalBitmap account, so a decoder that
        // still carried these fields would silently misread every instance.
        it('should no longer carry withdrawal root or tree index', () => {
            const testInstance: Instance = {
                discriminator: 0,
                bump: 250,
                version: 1,
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                admin: TEST_ADDRESSES.ADMIN,
            };

            const codec = getInstanceCodec();
            const decodedInstance = codec.decode(codec.encode(testInstance));

            expect(decodedInstance).not.toHaveProperty('withdrawalTransactionsRoot');
            expect(decodedInstance).not.toHaveProperty('currentTreeIndex');
        });
    });

    describe('Size validation', () => {
        it('should report correct account size (67 bytes)', () => {
            const accountSize = getInstanceEncoder().fixedSize;
            expect(accountSize).toBe(EXPECTED_SIZE);
        });

        it('should validate encoded data matches expected size', () => {
            const testInstance: Instance = {
                discriminator: 0,
                bump: 250,
                version: 1,
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                admin: TEST_ADDRESSES.ADMIN,
            };

            const encoder = getInstanceEncoder();
            const encodedData = encoder.encode(testInstance);

            expect(encodedData).toHaveLength(EXPECTED_SIZE);
            expect(encoder.fixedSize).toBe(EXPECTED_SIZE);
        });

        it('should validate size consistency across multiple instances', () => {
            const testInstances: Instance[] = [
                {
                    discriminator: 0,
                    bump: 100,
                    version: 1,
                    instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
                    admin: TEST_ADDRESSES.ADMIN,
                },
                {
                    discriminator: 255,
                    bump: 255,
                    version: 255,
                    instanceSeed: TEST_ADDRESSES.INSTANCE_SEED_2,
                    admin: TEST_ADDRESSES.WALLET,
                },
                {
                    discriminator: 127,
                    bump: 50,
                    version: 10,
                    instanceSeed: TEST_ADDRESSES.USDC_MINT,
                    admin: TEST_ADDRESSES.OPERATOR,
                },
            ];

            const encoder = getInstanceEncoder();

            for (const instance of testInstances) {
                const encodedData = encoder.encode(instance);
                expect(encodedData).toHaveLength(EXPECTED_SIZE);
            }
        });
    });
});
