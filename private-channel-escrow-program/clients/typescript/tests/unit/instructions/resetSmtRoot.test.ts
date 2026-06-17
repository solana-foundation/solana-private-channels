import { expect } from '@jest/globals';
import {
    getResetSmtRootInstructionAsync,
    getResetSmtRootInstructionDataCodec,
    RESET_SMT_ROOT_DISCRIMINATOR,
    findOperatorPda,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, EXPECTED_PROGRAM_ADDRESS } from '../../setup/mocks';
import { AccountRole, type Address } from '@solana/kit';

describe('resetSmtRoot', () => {
    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator (8)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            const decodedData = getResetSmtRootInstructionDataCodec().decode(instruction.data);

            // Verify discriminator is 8 as defined in the program
            expect(decodedData.discriminator).toBe(RESET_SMT_ROOT_DISCRIMINATOR);
            expect(decodedData.discriminator).toBe(8);
        });

        it('should encode discriminator and expectedCurrentTreeIndex', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            const decodedData = getResetSmtRootInstructionDataCodec().decode(instruction.data);

            // ResetSmtRoot carries the discriminator plus the expected tree index
            expect(Object.keys(decodedData)).toEqual(['discriminator', 'expectedCurrentTreeIndex']);
            expect(typeof decodedData.discriminator).toBe('number');
            expect(decodedData.expectedCurrentTreeIndex).toBe(0n);
        });

        it('should decode instruction data correctly', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            // Decode the instruction data
            const decodedData = getResetSmtRootInstructionDataCodec().decode(instruction.data);

            // Verify fields are decoded correctly
            expect(decodedData.discriminator).toBe(RESET_SMT_ROOT_DISCRIMINATOR);
            expect(typeof decodedData.discriminator).toBe('number');

            // Re-encode and verify it matches
            const reEncodedData = getResetSmtRootInstructionDataCodec().encode({ expectedCurrentTreeIndex: 0n });
            expect(reEncodedData).toEqual(instruction.data);
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: payer, operator, instance, operatorPda, eventAuthority, privateChannelEscrowProgram', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            // Based on program instruction definition, ResetSmtRoot should have 6 accounts
            expect(instruction.accounts).toHaveLength(6);

            // Account 0: payer (WritableSigner)
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.address).toBe(TEST_ADDRESSES.PAYER);

            // Account 1: operator (ReadonlySigner)
            const operatorAccount = instruction.accounts[1];
            expect(operatorAccount.address).toBe(TEST_ADDRESSES.OPERATOR);

            // Account 2: instance (Writable)
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.address).toBe(TEST_ADDRESSES.INSTANCE);

            // Account 3: operatorPda (Readonly PDA - auto-derived)
            const operatorPdaAccount = instruction.accounts[3];
            expect(operatorPdaAccount.address).toBeDefined();

            // Account 4: eventAuthority (Readonly)
            const eventAuthorityAccount = instruction.accounts[4];
            expect(eventAuthorityAccount.address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Account 5: privateChannelEscrowProgram (Readonly)
            const privateChannelEscrowProgramAccount = instruction.accounts[5];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            // Account 0: payer - should be WritableSigner
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);

            // Account 1: operator - should be ReadonlySigner
            const operatorAccount = instruction.accounts[1];
            expect(operatorAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 2: instance - should be Writable (PDA, not a signer)
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.role).toBe(AccountRole.WRITABLE);

            // Account 3: operatorPda - should be Readonly (PDA, not a signer)
            const operatorPdaAccount = instruction.accounts[3];
            expect(operatorPdaAccount.role).toBe(AccountRole.READONLY);

            // Account 4: eventAuthority - should be Readonly (PDA, not a signer)
            const eventAuthorityAccount = instruction.accounts[4];
            expect(eventAuthorityAccount.role).toBe(AccountRole.READONLY);

            // Account 5: privateChannelEscrowProgram - should be Readonly
            const privateChannelEscrowProgramAccount = instruction.accounts[5];
            expect(privateChannelEscrowProgramAccount.role).toBe(AccountRole.READONLY);
        });

        it('should use correct program addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
            expect(instruction.programAddress).toBe(EXPECTED_PROGRAM_ADDRESS);

            // Verify eventAuthority uses the correct address
            const eventAuthorityAccount = instruction.accounts[4];
            expect(eventAuthorityAccount.address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Verify privateChannelEscrowProgram uses the correct address
            const privateChannelEscrowProgramAccount = instruction.accounts[5];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });
    });

    describe('Automatic PDA derivation', () => {
        it('should automatically derive operatorPda when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            // Get expected operator PDA using findOperatorPda
            const [expectedOperatorPda] = await findOperatorPda({
                instance: TEST_ADDRESSES.INSTANCE,
                wallet: TEST_ADDRESSES.OPERATOR,
            });

            // Generate instruction without providing operatorPda - should be auto-derived
            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
                // Not providing operatorPda - should be auto-derived
            });

            // Verify the automatically derived operatorPda matches expected address
            expect(instruction.accounts[3].address).toBe(expectedOperatorPda);
        });

        it('should use default eventAuthority and privateChannelEscrowProgram when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
                // Not providing eventAuthority or privateChannelEscrowProgram - should use defaults
            });

            // Verify default eventAuthority is used
            const eventAuthorityAccount = instruction.accounts[4];
            expect(eventAuthorityAccount.address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Verify default privateChannelEscrowProgram is used
            const privateChannelEscrowProgramAccount = instruction.accounts[5];
            expect(privateChannelEscrowProgramAccount.address).toBe('GokvZqD2yP696rzNBNbQvcZ4VsLW7jNvFXU1kW9m7k83');
        });

        it('should use provided PDAs when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            // Use different addresses to override auto-derivation
            const overriddenOperatorPda = TEST_ADDRESSES.OPERATOR; // Use as override
            const overriddenEventAuthority = TEST_ADDRESSES.ADMIN; // Use as override
            const overriddenPrivateChannelEscrowProgram = TEST_ADDRESSES.USDC_MINT; // Use as override

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
                operatorPda: overriddenOperatorPda,
                eventAuthority: overriddenEventAuthority,
                privateChannelEscrowProgram: overriddenPrivateChannelEscrowProgram,
            });

            // Verify the provided addresses are used instead of auto-derived ones
            expect(instruction.accounts[3].address).toBe(overriddenOperatorPda);
            expect(instruction.accounts[4].address).toBe(overriddenEventAuthority);
            expect(instruction.accounts[5].address).toBe(overriddenPrivateChannelEscrowProgram);
        });
    });

    describe('Operator validation', () => {
        it('should require operator to be a signer', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
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
                const instruction = await getResetSmtRootInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedCurrentTreeIndex: 0n,
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

                const instruction = await getResetSmtRootInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedCurrentTreeIndex: 0n,
                });

                // Verify operatorPda is derived correctly for this operator
                const operatorPdaAccount = instruction.accounts[3];
                expect(operatorPdaAccount.address).toBe(expectedOperatorPda);
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
                const instruction = await getResetSmtRootInstructionAsync({
                    payer,
                    operator,
                    instance: instanceAddress,
                    expectedCurrentTreeIndex: 0n,
                });

                // Verify instance account uses the correct address
                const instanceAccount = instruction.accounts[2];
                expect(instanceAccount.address).toBe(instanceAddress);
                expect(instanceAccount.role).toBe(AccountRole.WRITABLE);
            }
        });

        it('should maintain consistent account ordering', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const operator = mockTransactionSigner(TEST_ADDRESSES.OPERATOR);

            const instruction = await getResetSmtRootInstructionAsync({
                payer,
                operator,
                instance: TEST_ADDRESSES.INSTANCE,
                expectedCurrentTreeIndex: 0n,
            });

            // Verify account ordering is consistent
            expect(instruction.accounts).toHaveLength(6);

            // Check each account position has the expected role
            expect(instruction.accounts[0].role).toBe(AccountRole.WRITABLE_SIGNER); // payer
            expect(instruction.accounts[1].role).toBe(AccountRole.READONLY_SIGNER); // operator
            expect(instruction.accounts[2].role).toBe(AccountRole.WRITABLE); // instance
            expect(instruction.accounts[3].role).toBe(AccountRole.READONLY); // operatorPda
            expect(instruction.accounts[4].role).toBe(AccountRole.READONLY); // eventAuthority
            expect(instruction.accounts[5].role).toBe(AccountRole.READONLY); // privateChannelEscrowProgram
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
                const instruction = await getResetSmtRootInstructionAsync({
                    payer,
                    operator,
                    instance: TEST_ADDRESSES.INSTANCE,
                    expectedCurrentTreeIndex: 0n,
                });

                // Verify payer account uses the correct address
                const payerAccount = instruction.accounts[0];
                expect(payerAccount.address).toBe(payer.address);
                expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);
            }
        });
    });
});
