import { expect } from '@jest/globals';
import {
    getCreateInstanceInstructionAsync,
    findInstancePda,
    getCreateInstanceInstructionDataCodec,
    CREATE_INSTANCE_DISCRIMINATOR,
    findEventAuthorityPda,
    findWithdrawalBitmapPda,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, EXPECTED_PROGRAM_ADDRESS } from '../../setup/mocks';
import { AccountRole } from '@solana/kit';
import { SYSTEM_PROGRAM_ADDRESS } from '@solana-program/system';

describe('createInstance', () => {
    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator (0), bump, and instanceSeed', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);
            const testBump = 42;

            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
                bump: testBump,
            });

            const decodedData = getCreateInstanceInstructionDataCodec().decode(instruction.data);

            // Verify discriminator is 0 as defined in the program
            expect(decodedData.discriminator).toBe(CREATE_INSTANCE_DISCRIMINATOR);
            expect(decodedData.discriminator).toBe(0);

            // Verify bump is correctly encoded
            expect(decodedData.bump).toBe(testBump);

            // Verify instanceSeed is correctly encoded
            expect(instruction.accounts[2].address).toBe(TEST_ADDRESSES.INSTANCE_SEED);
        });

        it('should handle bump parameter correctly (u8)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            // Get a valid instance PDA to use (so auto-derivation doesn't override our bump)
            const instancePda = await findInstancePda({
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED,
            });

            // Test various valid u8 values (1-255, avoiding 0 due to falsy check in generated code)
            const testBumps = [1, 42, 127, 200, 254, 255];

            for (const testBump of testBumps) {
                const instruction = await getCreateInstanceInstructionAsync({
                    payer,
                    admin,
                    instance: instancePda,
                    instanceSeed,
                    bump: testBump,
                });

                const decodedData = getCreateInstanceInstructionDataCodec().decode(instruction.data);
                expect(decodedData.bump).toBe(testBump);
            }
        });

        it('should handle instanceSeed parameter correctly (publicKey)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

            // Test with different valid instanceSeed addresses
            const testInstanceSeeds = [
                mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED),
                mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED_2),
                mockTransactionSigner(TEST_ADDRESSES.USDC_MINT),
                mockTransactionSigner(TEST_ADDRESSES.WRAPPED_SOL),
            ];

            for (const instanceSeed of testInstanceSeeds) {
                const instruction = await getCreateInstanceInstructionAsync({
                    payer,
                    admin,
                    instanceSeed,
                });

                expect(instruction.accounts[2].address).toBe(instanceSeed.address);
            }
        });

        it('should decode instruction data correctly', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const testBump = 150;
            const testInstanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED_2);

            // Create instruction with specific data
            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed: testInstanceSeed,
                bump: testBump,
            });

            // Decode the instruction data
            const decodedData = getCreateInstanceInstructionDataCodec().decode(instruction.data);

            // Verify all fields are decoded correctly
            expect(decodedData.discriminator).toBe(CREATE_INSTANCE_DISCRIMINATOR);
            expect(decodedData.bump).toBe(testBump);
            expect(instruction.accounts[2].address).toBe(testInstanceSeed.address);

            // The bitmap bump is auto-derived alongside its account.
            const [, expectedBitmapBump] = await findWithdrawalBitmapPda({
                instance: instruction.accounts[3].address,
            });
            expect(decodedData.bitmapBump).toBe(expectedBitmapBump);

            // Verify data types
            expect(typeof decodedData.discriminator).toBe('number');
            expect(typeof decodedData.bump).toBe('number');
            expect(typeof decodedData.bitmapBump).toBe('number');

            // Re-encode and verify it matches
            const reEncodedData = getCreateInstanceInstructionDataCodec().encode({
                bump: testBump,
                bitmapBump: expectedBitmapBump,
            });
            expect(reEncodedData).toEqual(instruction.data);
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: payer, admin, instanceSeed, instance, withdrawalBitmap, systemProgram, eventAuthority, privateChannelEscrowProgram', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
            });

            // Based on program instruction definition, CreateInstance should have 6 accounts
            expect(instruction.accounts).toHaveLength(8);

            // Account 0: payer (WritableSigner)
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.address).toBe(TEST_ADDRESSES.PAYER);

            // Account 1: admin (ReadonlySigner)
            const adminAccount = instruction.accounts[1];
            expect(adminAccount.address).toBe(TEST_ADDRESSES.ADMIN);

            // Account 2: instanceSeed (ReadonlySigner)
            const instanceSeedAccount = instruction.accounts[2];
            expect(instanceSeedAccount.address).toBe(TEST_ADDRESSES.INSTANCE_SEED);

            // Account 3: instance (Writable PDA)
            const instanceAccount = instruction.accounts[3];
            expect(instanceAccount.address).toBeDefined();

            // Account 4: withdrawalBitmap (Writable PDA, derived from the instance)
            const [expectedBitmapPda] = await findWithdrawalBitmapPda({
                instance: instanceAccount.address,
            });
            expect(instruction.accounts[4].address).toBe(expectedBitmapPda);

            // Account 5: systemProgram (Readonly)
            const systemProgramAccount = instruction.accounts[5];
            expect(systemProgramAccount.address).toBe(SYSTEM_PROGRAM_ADDRESS);

            // Account 5: eventAuthority (Readonly PDA)
            const eventAuthorityAccount = instruction.accounts[6];
            expect(eventAuthorityAccount.address).toBeDefined();

            // Account 6: privateChannelEscrowProgram (Readonly)
            const privateChannelEscrowProgramAccount = instruction.accounts[7];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
            });

            // Account 0: payer - should be WritableSigner
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);

            // Account 1: admin - should be ReadonlySigner
            const adminAccount = instruction.accounts[1];
            expect(adminAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 2: instanceSeed (ReadonlySigner)
            const instanceSeedAccount = instruction.accounts[2];
            expect(instanceSeedAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 3: instance - should be Writable (PDA, not a signer)
            const instanceAccount = instruction.accounts[3];
            expect(instanceAccount.role).toBe(AccountRole.WRITABLE);

            // Account 4: withdrawalBitmap - should be Writable (created here)
            expect(instruction.accounts[4].role).toBe(AccountRole.WRITABLE);

            // Account 5: systemProgram - should be Readonly
            const systemProgramAccount = instruction.accounts[5];
            expect(systemProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 5: eventAuthority - should be Readonly (PDA, not a signer)
            const eventAuthorityAccount = instruction.accounts[6];
            expect(eventAuthorityAccount.role).toBe(AccountRole.READONLY);

            // Account 6: privateChannelEscrowProgram - should be Readonly
            const privateChannelEscrowProgramAccount = instruction.accounts[7];
            expect(privateChannelEscrowProgramAccount.role).toBe(AccountRole.READONLY);
        });

        it('should use correct program addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
            expect(instruction.programAddress).toBe(EXPECTED_PROGRAM_ADDRESS);

            // Verify systemProgram uses the correct address
            const systemProgramAccount = instruction.accounts[5];
            expect(systemProgramAccount.address).toBe(SYSTEM_PROGRAM_ADDRESS);

            // Verify privateChannelEscrowProgram uses the correct address
            const privateChannelEscrowProgramAccount = instruction.accounts[7];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });

        it('should handle systemProgram address', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            // Test with default systemProgram (should auto-fill)
            const instruction1 = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
            });

            const systemProgramAccount1 = instruction1.accounts[5];
            expect(systemProgramAccount1.address).toBe(SYSTEM_PROGRAM_ADDRESS);

            // Test with explicitly provided systemProgram
            const instruction2 = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
                systemProgram: SYSTEM_PROGRAM_ADDRESS,
            });

            const systemProgramAccount2 = instruction2.accounts[5];
            expect(systemProgramAccount2.address).toBe(SYSTEM_PROGRAM_ADDRESS);
        });

        it('should handle eventAuthority PDA derivation', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            // Test with default eventAuthority (should auto-derive)
            const instruction1 = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
            });

            // Get expected eventAuthority PDA
            const [expectedEventAuthorityPda] = await findEventAuthorityPda();

            const eventAuthorityAccount1 = instruction1.accounts[6];
            expect(eventAuthorityAccount1.address).toBe(expectedEventAuthorityPda);

            // Test with explicitly provided eventAuthority
            const customEventAuthority = await findEventAuthorityPda();
            const instruction2 = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
                eventAuthority: customEventAuthority[0],
            });

            const eventAuthorityAccount2 = instruction2.accounts[6];
            expect(eventAuthorityAccount2.address).toBe(customEventAuthority[0]);
        });
    });

    describe('Automatic instance PDA derivation', () => {
        it('should automatically derive instance PDA when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            // Get expected instance PDA using findInstancePda
            const [expectedInstancePda, expectedBump] = await findInstancePda({
                instanceSeed: instanceSeed.address,
            });

            // Generate instruction without providing instance - should be auto-derived from instanceId
            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
                bump: expectedBump,
                // Not providing instance - should be auto-derived from instanceId
            });

            // Verify the automatically derived instance PDA matches expected address
            expect(instruction.accounts[3].address).toBe(expectedInstancePda);
            expect(instruction.accounts[2].address).toBe(instanceSeed.address);
            const decodedData = getCreateInstanceInstructionDataCodec().decode(instruction.data);
            expect(decodedData.bump).toBe(expectedBump);
            expect(decodedData.discriminator).toBe(CREATE_INSTANCE_DISCRIMINATOR);
        });

        it('should automatically derive bump when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);

            // Get expected instance PDA using findInstancePda
            const [expectedInstancePda, expectedBump] = await findInstancePda({
                instanceSeed: instanceSeed.address,
            });

            // Generate instruction without providing instance - should be auto-derived from instanceId
            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
                // Not providing bump - should be auto-derived from instanceId
            });

            // Verify the automatically derived instance PDA matches expected address
            expect(instruction.accounts[3].address).toBe(expectedInstancePda);
            expect(instruction.accounts[2].address).toBe(instanceSeed.address);
            const decodedData = getCreateInstanceInstructionDataCodec().decode(instruction.data);
            expect(decodedData.bump).toBe(expectedBump);
            expect(decodedData.discriminator).toBe(CREATE_INSTANCE_DISCRIMINATOR);
        });

        it('should use provided instance address when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);
            const mockBump = 100;
            const overriddenInstancePda = await findInstancePda({
                instanceSeed: TEST_ADDRESSES.INSTANCE_SEED_2,
            });

            const instruction = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instance: overriddenInstancePda,
                instanceSeed,
                bump: mockBump,
            });

            // Verify the provided address is used instead of auto-derived one
            expect(instruction.accounts[3].address).toBe(overriddenInstancePda[0]);
            expect(instruction.accounts[2].address).toBe(instanceSeed.address);
            const decodedData = getCreateInstanceInstructionDataCodec().decode(instruction.data);
            expect(decodedData.bump).toBe(mockBump);
            expect(decodedData.discriminator).toBe(CREATE_INSTANCE_DISCRIMINATOR);
        });

        it('should derive different PDAs for different instance seeds', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instanceSeed = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED);
            const instanceSeed2 = mockTransactionSigner(TEST_ADDRESSES.INSTANCE_SEED_2);
            const instruction1 = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed,
            });

            const instruction2 = await getCreateInstanceInstructionAsync({
                payer,
                admin,
                instanceSeed: instanceSeed2,
            });

            expect(instruction1.accounts[3].address).not.toBe(instruction2.accounts[3].address);
        });
    });
});
