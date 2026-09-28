import { expect } from '@jest/globals';
import { address, AccountRole } from '@solana/kit';
import {
    getAllowMintInstructionAsync,
    getAllowMintInstructionDataCodec,
    findAllowedMintPda,
    ALLOW_MINT_DISCRIMINATOR,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, EXPECTED_PROGRAM_ADDRESS } from '../../setup/mocks';

const WITHDRAW_FEE = 1_000n;

describe('allowMint', () => {
    describe('Automatic allowedMint PDA derivation', () => {
        it('should automatically derive allowedMint PDA and bump when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            // Get expected allowedMint PDA using findAllowedMintPda
            const [expectedAllowedMintPda, expectedBump] = await findAllowedMintPda({
                instance,
                mint,
            });

            // Generate instruction without providing allowedMint - should be auto-derived
            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                // Not providing bump - should be auto-derived from instance + mint
                // Not providing allowedMint - should be auto-derived from instance + mint
            });

            // Verify the automatically derived allowedMint PDA matches expected address
            const allowedMintAccount = instruction.accounts.find(acc => acc.address === expectedAllowedMintPda);
            expect(allowedMintAccount).toBeDefined();
            expect(allowedMintAccount?.address).toBe(expectedAllowedMintPda);
            const decodedData = getAllowMintInstructionDataCodec().decode(instruction.data);
            expect(decodedData.bump).toBe(expectedBump);
            expect(decodedData.discriminator).toBe(ALLOW_MINT_DISCRIMINATOR);
        });

        it('should use provided allowedMint address when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;
            const bump = 255;
            const customAllowedMintPda = await findAllowedMintPda({
                instance,
                mint,
            });

            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                allowedMint: customAllowedMintPda,
                bump,
            });

            expect(instruction.accounts[4].address).toBe(customAllowedMintPda[0]);
        });

        it('should derive different PDAs for different mint addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint1 = TEST_ADDRESSES.WRAPPED_SOL;
            const mint2 = TEST_ADDRESSES.USDC_MINT;

            const instruction1 = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint: mint1,
            });

            const instruction2 = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint: mint2,
            });

            // Find the allowedMint accounts in each instruction
            const [expectedAllowedMint1] = await findAllowedMintPda({
                instance,
                mint: mint1,
            });
            const [expectedAllowedMint2] = await findAllowedMintPda({
                instance,
                mint: mint2,
            });

            expect(instruction1.accounts[4].address).not.toBe(instruction2.accounts[4].address);
            expect(instruction1.accounts[4].address).toBe(expectedAllowedMint1);
            expect(instruction2.accounts[4].address).toBe(expectedAllowedMint2);
        });

        it('should derive different PDAs for different instance addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance1 = address('7JttKuoVeFqzMkspfBvTxGiHjYr9dT4GozFvJC5A7Nfa');
            const instance2 = address('BMLRKsFtgM7tNPnSS1DMBjJWjqChaY5AKHkNNHBooqpE');
            const mint = TEST_ADDRESSES.MINT;

            const instruction1 = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance: instance1,
                mint,
            });

            const instruction2 = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance: instance2,
                mint,
            });

            // Find the allowedMint accounts in each instruction
            const [expectedAllowedMint1] = await findAllowedMintPda({
                instance: instance1,
                mint,
            });
            const [expectedAllowedMint2] = await findAllowedMintPda({
                instance: instance2,
                mint,
            });

            expect(instruction1.accounts[4].address).not.toBe(instruction2.accounts[4].address);
            expect(instruction1.accounts[4].address).toBe(expectedAllowedMint1);
            expect(instruction2.accounts[4].address).toBe(expectedAllowedMint2);
        });

        it('should automatically derive instanceAta when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            // Generate instruction without providing instanceAta - should be auto-derived
            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                // Not providing instanceAta - should be auto-derived from instance + mint
            });

            // The instanceAta should be account index 5 based on the generated code account order
            const instanceAtaAccount = instruction.accounts[5];
            expect(instanceAtaAccount).toBeDefined();

            // Verify it's writable (required for instanceAta)
            expect(instanceAtaAccount.role).toBe(AccountRole.WRITABLE);

            // The address should be deterministically derived from instance + tokenProgram + mint
            // We can verify this by checking that the address is a valid base58 string
            expect(typeof instanceAtaAccount.address).toBe('string');
            expect(instanceAtaAccount.address.length).toBeGreaterThan(32); // Valid address
        });

        it('should use provided instanceAta when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;
            const customInstanceAta = TEST_ADDRESSES.INSTANCE_ATA;
            const bump = 255;

            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                instanceAta: customInstanceAta,
                bump,
            });

            // Verify the provided instanceAta address is used instead of auto-derived one
            const instanceAtaAccount = instruction.accounts[5];
            expect(instanceAtaAccount.address).toBe(customInstanceAta);
            expect(instanceAtaAccount.role).toBe(AccountRole.WRITABLE);
        });
    });

    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator (1) and bump parameter', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;
            const testBump = 42;

            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                bump: testBump,
            });

            const decodedData = getAllowMintInstructionDataCodec().decode(instruction.data);

            // Verify discriminator is 1 as defined in the program (AllowMint = 1)
            expect(decodedData.discriminator).toBe(ALLOW_MINT_DISCRIMINATOR);
            expect(decodedData.discriminator).toBe(1);

            // Verify bump is correctly encoded
            expect(decodedData.bump).toBe(testBump);
        });

        it('should handle bump parameter correctly (u8)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            // Test various valid u8 values (0-255)
            const testBumps = [1, 42, 127, 200, 254, 255];

            for (const testBump of testBumps) {
                const instruction = await getAllowMintInstructionAsync({
                    withdrawFee: WITHDRAW_FEE,
                    payer,
                    admin,
                    instance,
                    mint,
                    bump: testBump,
                });

                const decodedData = getAllowMintInstructionDataCodec().decode(instruction.data);
                expect(decodedData.bump).toBe(testBump);
            }
        });

        it('should decode instruction data correctly', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.USDC_MINT;
            const testBump = 150;

            // Create instruction with specific data
            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                bump: testBump,
            });

            // Decode the instruction data
            const decodedData = getAllowMintInstructionDataCodec().decode(instruction.data);

            // Verify all fields are decoded correctly
            expect(decodedData.discriminator).toBe(ALLOW_MINT_DISCRIMINATOR);
            expect(decodedData.bump).toBe(testBump);
            expect(decodedData.withdrawFee).toBe(WITHDRAW_FEE);

            // Verify data types
            expect(typeof decodedData.discriminator).toBe('number');
            expect(typeof decodedData.bump).toBe('number');

            // Re-encode and verify it matches
            const reEncodedData = getAllowMintInstructionDataCodec().encode({
                bump: testBump,
                withdrawFee: WITHDRAW_FEE,
            });
            expect(reEncodedData).toEqual(instruction.data);
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: payer, admin, instance, mint, allowedMint, instanceAta, systemProgram, tokenProgram, associatedTokenProgram, eventAuthority, privateChannelEscrowProgram', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;
            const bump = 255;

            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                bump,
            });

            // Based on program instruction definition, AllowMint should have 11 accounts
            expect(instruction.accounts).toHaveLength(11);

            // Account 0: payer (WritableSigner)
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.address).toBe(TEST_ADDRESSES.PAYER);

            // Account 1: admin (ReadonlySigner)
            const adminAccount = instruction.accounts[1];
            expect(adminAccount.address).toBe(TEST_ADDRESSES.ADMIN);

            // Account 2: instance (Readonly)
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.address).toBe(instance);

            // Account 3: mint (Readonly)
            const mintAccount = instruction.accounts[3];
            expect(mintAccount.address).toBe(mint);

            // Account 4: allowedMint (Writable PDA)
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBeDefined();

            // Account 5: instanceAta (Writable)
            const instanceAtaAccount = instruction.accounts[5];
            expect(instanceAtaAccount.address).toBeDefined();

            // Account 6: systemProgram (Readonly)
            const systemProgramAccount = instruction.accounts[6];
            expect(systemProgramAccount.address).toBe('11111111111111111111111111111111');

            // Account 7: tokenProgram (Readonly)
            const tokenProgramAccount = instruction.accounts[7];
            expect(tokenProgramAccount.address).toBe('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

            // Account 8: associatedTokenProgram (Readonly)
            const associatedTokenProgramAccount = instruction.accounts[8];
            expect(associatedTokenProgramAccount.address).toBe('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL');

            // Account 9: eventAuthority (Readonly PDA)
            const eventAuthorityAccount = instruction.accounts[9];
            expect(eventAuthorityAccount.address).toBeDefined();

            // Account 10: privateChannelEscrowProgram (Readonly)
            const privateChannelEscrowProgramAccount = instruction.accounts[10];
            expect(privateChannelEscrowProgramAccount.address).toBe(EXPECTED_PROGRAM_ADDRESS);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;
            const bump = 255;

            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                bump,
            });

            // Account 0: payer - should be WritableSigner
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);

            // Account 1: admin - should be ReadonlySigner
            const adminAccount = instruction.accounts[1];
            expect(adminAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 2: instance - should be Readonly
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.role).toBe(AccountRole.READONLY);

            // Account 3: mint - should be Readonly
            const mintAccount = instruction.accounts[3];
            expect(mintAccount.role).toBe(AccountRole.READONLY);

            // Account 4: allowedMint - should be Writable (PDA, not a signer)
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.role).toBe(AccountRole.WRITABLE);

            // Account 5: instanceAta - should be Writable (not a signer)
            const instanceAtaAccount = instruction.accounts[5];
            expect(instanceAtaAccount.role).toBe(AccountRole.WRITABLE);

            // Account 6: systemProgram - should be Readonly
            const systemProgramAccount = instruction.accounts[6];
            expect(systemProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 7: tokenProgram - should be Readonly
            const tokenProgramAccount = instruction.accounts[7];
            expect(tokenProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 8: associatedTokenProgram - should be Readonly
            const associatedTokenProgramAccount = instruction.accounts[8];
            expect(associatedTokenProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 9: eventAuthority - should be Readonly (PDA, not a signer)
            const eventAuthorityAccount = instruction.accounts[9];
            expect(eventAuthorityAccount.role).toBe(AccountRole.READONLY);

            // Account 10: privateChannelEscrowProgram - should be Readonly
            const privateChannelEscrowProgramAccount = instruction.accounts[10];
            expect(privateChannelEscrowProgramAccount.role).toBe(AccountRole.READONLY);
        });

        it('should use correct program addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;
            const bump = 255;

            const instruction = await getAllowMintInstructionAsync({
                withdrawFee: WITHDRAW_FEE,
                payer,
                admin,
                instance,
                mint,
                bump,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(EXPECTED_PROGRAM_ADDRESS);

            // Verify systemProgram uses the correct address
            const systemProgramAccount = instruction.accounts[6];
            expect(systemProgramAccount.address).toBe('11111111111111111111111111111111');

            // Verify tokenProgram uses the correct address
            const tokenProgramAccount = instruction.accounts[7];
            expect(tokenProgramAccount.address).toBe('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

            // Verify associatedTokenProgram uses the correct address
            const associatedTokenProgramAccount = instruction.accounts[8];
            expect(associatedTokenProgramAccount.address).toBe('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL');

            // Verify privateChannelEscrowProgram uses the correct address
            const privateChannelEscrowProgramAccount = instruction.accounts[10];
            expect(privateChannelEscrowProgramAccount.address).toBe(EXPECTED_PROGRAM_ADDRESS);
        });
    });
});
