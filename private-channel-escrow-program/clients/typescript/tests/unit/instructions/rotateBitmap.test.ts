import { expect } from '@jest/globals';
import {
    getRotateBitmapInstructionAsync,
    getRotateBitmapInstructionDataCodec,
    ROTATE_BITMAP_DISCRIMINATOR,
    findOperatorPda,
    findWithdrawalBitmapPda,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, EXPECTED_PROGRAM_ADDRESS } from '../../setup/mocks';
import { AccountRole, type Address } from '@solana/kit';

describe('rotateBitmap', () => {
    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator (8)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            const decodedData = getRotateBitmapInstructionDataCodec().decode(instruction.data);

            // Verify discriminator is 8 as defined in the program
            expect(decodedData.discriminator).toBe(ROTATE_BITMAP_DISCRIMINATOR);
            expect(decodedData.discriminator).toBe(8);
        });

        it('should encode discriminator and expectedGeneration', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            const decodedData = getRotateBitmapInstructionDataCodec().decode(instruction.data);

            // RotateBitmap carries the discriminator plus the expected generation
            expect(Object.keys(decodedData)).toEqual(['discriminator', 'expectedGeneration']);
            expect(typeof decodedData.discriminator).toBe('number');
            expect(decodedData.expectedGeneration).toBe(0n);
        });

        it('should decode instruction data correctly', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            // Decode the instruction data
            const decodedData = getRotateBitmapInstructionDataCodec().decode(instruction.data);

            // Verify fields are decoded correctly
            expect(decodedData.discriminator).toBe(ROTATE_BITMAP_DISCRIMINATOR);
            expect(typeof decodedData.discriminator).toBe('number');

            // Re-encode and verify it matches
            const reEncodedData = getRotateBitmapInstructionDataCodec().encode({ expectedGeneration: 0n });
            expect(reEncodedData).toEqual(instruction.data);
        });

        // The generation is the replay guard, so a non-zero value must survive
        // the round trip rather than being pinned at the default.
        it('should carry a non-zero expectedGeneration', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            for (const expectedGeneration of [1n, 42n, 18446744073709551615n]) {
                const instruction = await getRotateBitmapInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedGeneration,
                });

                const decodedData = getRotateBitmapInstructionDataCodec().decode(instruction.data);
                expect(decodedData.expectedGeneration).toBe(expectedGeneration);
            }
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: payer, operator, instance, withdrawalBitmap, operatorPda, eventAuthority, privateChannelEscrowProgram', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            // Based on program instruction definition, RotateBitmap should have 7 accounts
            expect(instruction.accounts).toHaveLength(7);

            // Account 0: payer (WritableSigner)
            expect(instruction.accounts[0].address).toBe(TEST_ADDRESSES.PAYER);

            // Account 1: operator (ReadonlySigner)
            expect(instruction.accounts[1].address).toBe(TEST_ADDRESSES.OPERATOR);

            // Account 2: instance (Readonly)
            expect(instruction.accounts[2].address).toBe(TEST_ADDRESSES.INSTANCE);

            // Account 3: withdrawalBitmap (Writable PDA - auto-derived)
            expect(instruction.accounts[3].address).toBeDefined();

            // Account 4: operatorPda (Readonly PDA - auto-derived)
            expect(instruction.accounts[4].address).toBeDefined();

            // Account 5: eventAuthority (Readonly)
            expect(instruction.accounts[5].address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Account 6: privateChannelEscrowProgram (Readonly)
            expect(instruction.accounts[6].address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            expect(instruction.accounts[0].role).toBe(AccountRole.WRITABLE_SIGNER); // payer
            expect(instruction.accounts[1].role).toBe(AccountRole.READONLY_SIGNER); // operator
            // The instance is only read now: replay state lives in the bitmap.
            expect(instruction.accounts[2].role).toBe(AccountRole.READONLY); // instance
            expect(instruction.accounts[3].role).toBe(AccountRole.WRITABLE); // withdrawalBitmap
            expect(instruction.accounts[4].role).toBe(AccountRole.READONLY); // operatorPda
            expect(instruction.accounts[5].role).toBe(AccountRole.READONLY); // eventAuthority
            expect(instruction.accounts[6].role).toBe(AccountRole.READONLY); // privateChannelEscrowProgram
        });

        it('should use correct program addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
            expect(instruction.programAddress).toBe(EXPECTED_PROGRAM_ADDRESS);

            // Verify eventAuthority uses the correct address
            expect(instruction.accounts[5].address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Verify privateChannelEscrowProgram uses the correct address
            expect(instruction.accounts[6].address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });
    });

    describe('Automatic PDA derivation', () => {
        it('should automatically derive withdrawalBitmap when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const [expectedBitmapPda] = await findWithdrawalBitmapPda({
                instance: TEST_ADDRESSES.INSTANCE,
            });

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
                // Not providing withdrawalBitmap - should be auto-derived
            });

            expect(instruction.accounts[3].address).toBe(expectedBitmapPda);
        });

        it('should automatically derive operatorPda when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            // Get expected operator PDA using findOperatorPda
            const [expectedOperatorPda] = await findOperatorPda({
                instance: TEST_ADDRESSES.INSTANCE,
                wallet: TEST_ADDRESSES.OPERATOR,
            });

            // Generate instruction without providing operatorPda - should be auto-derived
            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
                // Not providing operatorPda - should be auto-derived
            });

            // Verify the automatically derived operatorPda matches expected address
            expect(instruction.accounts[4].address).toBe(expectedOperatorPda);
        });

        it('should use default eventAuthority and privateChannelEscrowProgram when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
                // Not providing eventAuthority or privateChannelEscrowProgram - should use defaults
            });

            // Verify default eventAuthority is used
            expect(instruction.accounts[5].address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Verify default privateChannelEscrowProgram is used
            expect(instruction.accounts[6].address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });

        it('should use provided PDAs when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            // Use different addresses to override auto-derivation
            const overriddenBitmap = TEST_ADDRESSES.WRAPPED_SOL;
            const overriddenOperatorPda = TEST_ADDRESSES.OPERATOR;
            const overriddenEventAuthority = TEST_ADDRESSES.ADMIN;
            const overriddenPrivateChannelEscrowProgram = TEST_ADDRESSES.USDC_MINT;

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
                withdrawalBitmap: overriddenBitmap,
                operatorPda: overriddenOperatorPda,
                eventAuthority: overriddenEventAuthority,
                privateChannelEscrowProgram: overriddenPrivateChannelEscrowProgram,
            });

            // Verify the provided addresses are used instead of auto-derived ones
            expect(instruction.accounts[3].address).toBe(overriddenBitmap);
            expect(instruction.accounts[4].address).toBe(overriddenOperatorPda);
            expect(instruction.accounts[5].address).toBe(overriddenEventAuthority);
            expect(instruction.accounts[6].address).toBe(overriddenPrivateChannelEscrowProgram);
        });
    });

    describe('Operator validation', () => {
        it('should require operator to be a signer', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            // Verify operator account is a signer
            const operatorAccount = instruction.accounts[1];
            expect(operatorAccount.role).toBe(AccountRole.READONLY_SIGNER);
            expect(operatorAccount.address).toBe(TEST_ADDRESSES.OPERATOR);
        });

        it('should handle different operator addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);

            // Test with different valid operator addresses
            const testOperators = [
                mockTransactionSigner(TEST_ADDRESSES.OPERATOR),
                mockTransactionSigner(TEST_ADDRESSES.ADMIN),
                mockTransactionSigner(TEST_ADDRESSES.WALLET),
            ];

            for (const operator of testOperators) {
                const instruction = await getRotateBitmapInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedGeneration: 0n,
                });

                // Verify operator account uses the correct address
                const operatorAccount = instruction.accounts[1];
                expect(operatorAccount.address).toBe(operator.address);
                expect(operatorAccount.role).toBe(AccountRole.READONLY_SIGNER);
            }
        });

        it('should automatically derive operatorPda based on operator address', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);

            // Test with different operators and verify operatorPda derives correctly
            const testOperators = [
                mockTransactionSigner(TEST_ADDRESSES.OPERATOR),
                mockTransactionSigner(TEST_ADDRESSES.ADMIN),
            ];

            for (const operator of testOperators) {
                // Get expected operator PDA for this operator
                const [expectedOperatorPda] = await findOperatorPda({
                    instance: TEST_ADDRESSES.INSTANCE,
                    wallet: operator.address,
                });

                const instruction = await getRotateBitmapInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedGeneration: 0n,
                });

                // Verify operatorPda is derived correctly for this operator
                expect(instruction.accounts[4].address).toBe(expectedOperatorPda);
            }
        });
    });

    describe('Parameter edge cases', () => {
        it('should handle different instance addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            // Test with different valid instance addresses
            const testInstances = [
                TEST_ADDRESSES.INSTANCE,
                TEST_ADDRESSES.INSTANCE_SEED,
                TEST_ADDRESSES.INSTANCE_SEED_2,
            ] as Address[];

            for (const instanceAddress of testInstances) {
                const instruction = await getRotateBitmapInstructionAsync({
                    payer,
                    operator,
                    instance: instanceAddress,
                    expectedGeneration: 0n,
                });

                // Verify instance account uses the correct address
                const instanceAccount = instruction.accounts[2];
                expect(instanceAccount.address).toBe(instanceAddress);
                expect(instanceAccount.role).toBe(AccountRole.READONLY);

                // The bitmap must follow the instance it was derived from.
                const [expectedBitmapPda] = await findWithdrawalBitmapPda({ instance: instanceAddress });
                expect(instruction.accounts[3].address).toBe(expectedBitmapPda);
            }
        });

        it('should maintain consistent account ordering', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getRotateBitmapInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedGeneration: 0n,
            });

            // Verify account ordering is consistent
            expect(instruction.accounts).toHaveLength(7);

            // Check each account position has the expected role
            expect(instruction.accounts[0].role).toBe(AccountRole.WRITABLE_SIGNER); // payer
            expect(instruction.accounts[1].role).toBe(AccountRole.READONLY_SIGNER); // operator
            expect(instruction.accounts[2].role).toBe(AccountRole.READONLY); // instance
            expect(instruction.accounts[3].role).toBe(AccountRole.WRITABLE); // withdrawalBitmap
            expect(instruction.accounts[4].role).toBe(AccountRole.READONLY); // operatorPda
            expect(instruction.accounts[5].role).toBe(AccountRole.READONLY); // eventAuthority
            expect(instruction.accounts[6].role).toBe(AccountRole.READONLY); // privateChannelEscrowProgram
        });

        it('should handle different payer addresses', async () => {
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            // Test with different valid payer addresses
            const testPayers = [
                mockTransactionSigner(TEST_ADDRESSES.PAYER),
                mockTransactionSigner(TEST_ADDRESSES.ADMIN),
                mockTransactionSigner(TEST_ADDRESSES.WALLET),
            ];

            for (const payer of testPayers) {
                const instruction = await getRotateBitmapInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedGeneration: 0n,
                });

                // Verify payer account uses the correct address
                const payerAccount = instruction.accounts[0];
                expect(payerAccount.address).toBe(payer.address);
                expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);
            }
        });
    });
});
