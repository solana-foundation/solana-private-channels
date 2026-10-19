import { expect } from '@jest/globals';
import {
    getDepositInstructionAsync,
    getDepositInstructionDataCodec,
    findAllowedMintPda,
    findDepositorPda,
    DEPOSIT_DISCRIMINATOR,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, EXPECTED_PROGRAM_ADDRESS } from '../../setup/mocks';
import { AccountRole, type Address } from '@solana/kit';
import { SYSTEM_PROGRAM_ADDRESS } from '@solana-program/system';
import { TOKEN_PROGRAM_ADDRESS, ASSOCIATED_TOKEN_PROGRAM_ADDRESS, findAssociatedTokenPda } from '@solana-program/token';
import { TOKEN_2022_PROGRAM_ADDRESS } from '@solana-program/token-2022';

describe('deposit', () => {
    // recipientDepositorPda has no default, since it follows the optional recipient.
    // Each call passes the PDA of the wallet credited in the channel.
    let walletDepositorPda: Address;
    let adminDepositorPda: Address;

    beforeAll(async () => {
        [walletDepositorPda] = await findDepositorPda({
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.WALLET,
        });
        [adminDepositorPda] = await findDepositorPda({
            instance: TEST_ADDRESSES.INSTANCE,
            wallet: TEST_ADDRESSES.ADMIN,
        });
    });

    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator (6), amount, and recipient', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const testAmount = 1000000n; // 1 USDC (6 decimals)
            const testRecipient = TEST_ADDRESSES.ADMIN;

            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: testAmount,
                recipient: testRecipient,
                recipientDepositorPda: adminDepositorPda,
            });

            const decodedData = getDepositInstructionDataCodec().decode(instruction.data);

            // Verify discriminator is 6 as defined in the program
            expect(decodedData.discriminator).toBe(DEPOSIT_DISCRIMINATOR);
            expect(decodedData.discriminator).toBe(6);

            // Verify amount is correctly encoded as u64
            expect(decodedData.amount).toBe(testAmount);

            // Verify recipient is correctly encoded as Option<Address>
            expect(decodedData.recipient).toEqual({ __option: 'Some', value: testRecipient });
        });

        it('should handle amount parameter correctly (u64)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            // Test various valid u64 values - both number and bigint inputs
            const testAmounts = [
                0n,
                1n,
                1000000n, // 1 USDC (6 decimals)
                1000000000n, // 1000 USDC
                9223372036854775807n, // max safe integer as bigint
                18446744073709551615n, // max u64 value
            ];

            for (const testAmount of testAmounts) {
                const instruction = await getDepositInstructionAsync({
                    payer,
                    user,
                    instance: TEST_ADDRESSES.INSTANCE,
                    mint: TEST_ADDRESSES.USDC_MINT,
                    userAta: TEST_ADDRESSES.INSTANCE_ATA,
                    amount: testAmount,
                    recipient: null,
                    recipientDepositorPda: walletDepositorPda,
                });

                const decodedData = getDepositInstructionDataCodec().decode(instruction.data);
                expect(decodedData.amount).toBe(testAmount);
                expect(typeof decodedData.amount).toBe('bigint');
            }

            // Test number input (should be converted to bigint)
            const numberAmount = 5000000; // 5 USDC
            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: numberAmount,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            const decodedData = getDepositInstructionDataCodec().decode(instruction.data);
            expect(decodedData.amount).toBe(BigInt(numberAmount));
        });

        it('should handle optional recipient parameter (Some/None)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const testAmount = 1000000n;

            // Test with None/null recipient
            const instructionWithNullRecipient = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: testAmount,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            const decodedDataNull = getDepositInstructionDataCodec().decode(instructionWithNullRecipient.data);
            expect(decodedDataNull.recipient).toEqual({ __option: 'None' });

            // Test with Some recipient
            const testRecipient = TEST_ADDRESSES.ADMIN;
            const instructionWithRecipient = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: testAmount,
                recipient: testRecipient,
                recipientDepositorPda: adminDepositorPda,
            });

            const decodedDataSome = getDepositInstructionDataCodec().decode(instructionWithRecipient.data);
            expect(decodedDataSome.recipient).toEqual({ __option: 'Some', value: testRecipient });

            // Test with different valid recipient addresses
            const testRecipients = [
                TEST_ADDRESSES.WALLET,
                TEST_ADDRESSES.ADMIN,
                TEST_ADDRESSES.OPERATOR,
                TEST_ADDRESSES.PAYER,
            ];

            for (const recipient of testRecipients) {
                const [recipientDepositorPda] = await findDepositorPda({
                    instance: TEST_ADDRESSES.INSTANCE,
                    wallet: recipient,
                });
                const instruction = await getDepositInstructionAsync({
                    payer,
                    user,
                    instance: TEST_ADDRESSES.INSTANCE,
                    mint: TEST_ADDRESSES.USDC_MINT,
                    userAta: TEST_ADDRESSES.INSTANCE_ATA,
                    amount: testAmount,
                    recipient,
                    recipientDepositorPda,
                });

                const decodedData = getDepositInstructionDataCodec().decode(instruction.data);
                expect(decodedData.recipient).toEqual({ __option: 'Some', value: recipient });
            }
        });

        it('should decode instruction data correctly', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const testAmount = 2500000n; // 2.5 USDC (6 decimals)
            const testRecipient = TEST_ADDRESSES.ADMIN;

            // Create instruction with specific data
            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: testAmount,
                recipient: testRecipient,
                recipientDepositorPda: adminDepositorPda,
            });

            // Decode the instruction data
            const decodedData = getDepositInstructionDataCodec().decode(instruction.data);

            // Verify all fields are decoded correctly
            expect(decodedData.discriminator).toBe(DEPOSIT_DISCRIMINATOR);
            expect(decodedData.amount).toBe(testAmount);
            expect(decodedData.recipient).toEqual({ __option: 'Some', value: testRecipient });

            // Verify data types
            expect(typeof decodedData.discriminator).toBe('number');
            expect(typeof decodedData.amount).toBe('bigint');
            expect(typeof decodedData.recipient).toBe('object');

            // Test with null recipient
            const instructionNullRecipient = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: testAmount,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            const decodedDataNull = getDepositInstructionDataCodec().decode(instructionNullRecipient.data);
            expect(decodedDataNull.recipient).toEqual({ __option: 'None' });

            // Re-encode and verify it matches original
            const reEncodedData = getDepositInstructionDataCodec().encode({
                amount: testAmount,
                recipient: testRecipient,
            });
            expect(reEncodedData).toEqual(instruction.data);
            const reEncodedDataNull = getDepositInstructionDataCodec().encode({
                amount: testAmount,
                recipient: null,
            });
            expect(reEncodedDataNull).toEqual(instructionNullRecipient.data);
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: payer, user, instance, mint, allowedMint, userAta, instanceAta, systemProgram, tokenProgram, associatedTokenProgram, eventAuthority, privateChannelEscrowProgram, userDepositorPda, recipientDepositorPda', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Based on program instruction definition, Deposit should have 14 accounts
            expect(instruction.accounts).toHaveLength(14);

            // Account 0: payer (WritableSigner)
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.address).toBe(TEST_ADDRESSES.PAYER);

            // Account 1: user (ReadonlySigner)
            const userAccount = instruction.accounts[1];
            expect(userAccount.address).toBe(TEST_ADDRESSES.WALLET);

            // Account 2: instance (Readonly)
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.address).toBe(TEST_ADDRESSES.INSTANCE);

            // Account 3: mint (Readonly)
            const mintAccount = instruction.accounts[3];
            expect(mintAccount.address).toBe(TEST_ADDRESSES.USDC_MINT);

            // Account 4: allowedMint (Readonly PDA - auto-derived)
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBeDefined();

            // Account 5: userAta (Writable)
            const userAtaAccount = instruction.accounts[5];
            expect(userAtaAccount.address).toBe(TEST_ADDRESSES.INSTANCE_ATA);

            // Account 6: instanceAta (Writable - auto-derived)
            const instanceAtaAccount = instruction.accounts[6];
            expect(instanceAtaAccount.address).toBeDefined();

            // Account 7: systemProgram (Readonly)
            const systemProgramAccount = instruction.accounts[7];
            expect(systemProgramAccount.address).toBe(SYSTEM_PROGRAM_ADDRESS);

            // Account 8: tokenProgram (Readonly)
            const tokenProgramAccount = instruction.accounts[8];
            expect(tokenProgramAccount.address).toBe(TOKEN_PROGRAM_ADDRESS);

            // Account 9: associatedTokenProgram (Readonly)
            const associatedTokenProgramAccount = instruction.accounts[9];
            expect(associatedTokenProgramAccount.address).toBe(ASSOCIATED_TOKEN_PROGRAM_ADDRESS);

            // Account 10: eventAuthority (Readonly)
            const eventAuthorityAccount = instruction.accounts[10];
            expect(eventAuthorityAccount.address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Account 11: privateChannelEscrowProgram (Readonly)
            const privateChannelEscrowProgramAccount = instruction.accounts[11];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
            expect(privateChannelEscrowProgramAccount.address).toBe(EXPECTED_PROGRAM_ADDRESS);

            // Account 12: userDepositorPda (Readonly PDA - auto-derived from user)
            expect(instruction.accounts[12].address).toBe(walletDepositorPda);

            // Account 13: recipientDepositorPda (Readonly - provided by the caller)
            expect(instruction.accounts[13].address).toBe(walletDepositorPda);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Account 0: payer - should be WritableSigner
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);

            // Account 1: user - should be ReadonlySigner
            const userAccount = instruction.accounts[1];
            expect(userAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 2: instance - should be Readonly
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.role).toBe(AccountRole.READONLY);

            // Account 3: mint - should be Readonly
            const mintAccount = instruction.accounts[3];
            expect(mintAccount.role).toBe(AccountRole.READONLY);

            // Account 4: allowedMint - should be Readonly (PDA, not a signer)
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.role).toBe(AccountRole.READONLY);

            // Account 5: userAta - should be Writable
            const userAtaAccount = instruction.accounts[5];
            expect(userAtaAccount.role).toBe(AccountRole.WRITABLE);

            // Account 6: instanceAta - should be Writable
            const instanceAtaAccount = instruction.accounts[6];
            expect(instanceAtaAccount.role).toBe(AccountRole.WRITABLE);

            // Account 7: systemProgram - should be Readonly
            const systemProgramAccount = instruction.accounts[7];
            expect(systemProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 8: tokenProgram - should be Readonly
            const tokenProgramAccount = instruction.accounts[8];
            expect(tokenProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 9: associatedTokenProgram - should be Readonly
            const associatedTokenProgramAccount = instruction.accounts[9];
            expect(associatedTokenProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 10: eventAuthority - should be Readonly
            const eventAuthorityAccount = instruction.accounts[10];
            expect(eventAuthorityAccount.role).toBe(AccountRole.READONLY);

            // Account 11: privateChannelEscrowProgram - should be Readonly
            const privateChannelEscrowProgramAccount = instruction.accounts[11];
            expect(privateChannelEscrowProgramAccount.role).toBe(AccountRole.READONLY);

            // Accounts 12 and 13: the depositor PDAs - should be Readonly
            expect(instruction.accounts[12].role).toBe(AccountRole.READONLY);
            expect(instruction.accounts[13].role).toBe(AccountRole.READONLY);
        });

        it('should use correct program addresses', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
            expect(instruction.programAddress).toBe(EXPECTED_PROGRAM_ADDRESS);

            // Verify systemProgram uses the correct address
            const systemProgramAccount = instruction.accounts[7];
            expect(systemProgramAccount.address).toBe(SYSTEM_PROGRAM_ADDRESS);

            // Verify tokenProgram uses the correct address
            const tokenProgramAccount = instruction.accounts[8];
            expect(tokenProgramAccount.address).toBe(TOKEN_PROGRAM_ADDRESS);

            // Verify associatedTokenProgram uses the correct address
            const associatedTokenProgramAccount = instruction.accounts[9];
            expect(associatedTokenProgramAccount.address).toBe(ASSOCIATED_TOKEN_PROGRAM_ADDRESS);

            // Verify eventAuthority uses the correct address
            const eventAuthorityAccount = instruction.accounts[10];
            expect(eventAuthorityAccount.address).toBe(TEST_ADDRESSES.EVENT_AUTHORITY);

            // Verify privateChannelEscrowProgram uses the correct address
            const privateChannelEscrowProgramAccount = instruction.accounts[11];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });
    });

    describe('Automatic PDA and ATA derivation', () => {
        it('should automatically derive allowedMint PDA when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            // Generate instruction without providing allowedMint - should be auto-derived
            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Get expected allowedMint PDA using findAllowedMintPda
            const [expectedAllowedMintPda] = await findAllowedMintPda({
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
            });

            // Verify the automatically derived allowedMint PDA matches expected address
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBe(expectedAllowedMintPda);
        });

        it('should automatically derive instance ATA when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const [expectedInstanceAta] = await findAssociatedTokenPda({
                mint: TEST_ADDRESSES.USDC_MINT,
                owner: TEST_ADDRESSES.INSTANCE,
                tokenProgram: TOKEN_PROGRAM_ADDRESS,
            });

            // Generate instruction without providing instanceAta - should be auto-derived
            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Verify the automatically derived instance ATA is defined
            const instanceAtaAccount = instruction.accounts[6];
            expect(instanceAtaAccount.address).toBe(expectedInstanceAta);
        });
        it('should automatically derive instance ATA when not provided (token 2022)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const [expectedInstanceAta] = await findAssociatedTokenPda({
                mint: TEST_ADDRESSES.USDC_MINT,
                owner: TEST_ADDRESSES.INSTANCE,
                tokenProgram: TOKEN_2022_PROGRAM_ADDRESS,
            });

            // Generate instruction without providing instanceAta - should be auto-derived
            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                tokenProgram: TOKEN_2022_PROGRAM_ADDRESS,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Verify the automatically derived instance ATA is defined
            const instanceAtaAccount = instruction.accounts[6];
            expect(instanceAtaAccount.address).toBe(expectedInstanceAta);

            // Verify Token 2022 program is used
            const tokenProgramAccount = instruction.accounts[8];
            expect(tokenProgramAccount.address).toBe(TOKEN_2022_PROGRAM_ADDRESS);
        });

        it('should use provided addresses when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const customAllowedMint = TEST_ADDRESSES.ALLOWED_MINT;
            const customInstanceAta = TEST_ADDRESSES.MINT; // Using a different address as custom

            // Generate instruction with provided allowedMint and instanceAta
            const instruction = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                allowedMint: customAllowedMint,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                instanceAta: customInstanceAta,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Verify the provided addresses are used instead of auto-derived ones
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBe(customAllowedMint);

            const instanceAtaAccount = instruction.accounts[6];
            expect(instanceAtaAccount.address).toBe(customInstanceAta);
        });

        it('should derive different PDAs/ATAs for different combinations', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            // First combination: INSTANCE + USDC_MINT
            const instruction1 = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.USDC_MINT,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Second combination: INSTANCE + WRAPPED_SOL
            const instruction2 = await getDepositInstructionAsync({
                payer,
                user,
                instance: TEST_ADDRESSES.INSTANCE,
                mint: TEST_ADDRESSES.WRAPPED_SOL,
                userAta: TEST_ADDRESSES.INSTANCE_ATA,
                amount: 1000000n,
                recipient: null,
                recipientDepositorPda: walletDepositorPda,
            });

            // Verify different mints produce different allowedMint PDAs
            const allowedMintAccount1 = instruction1.accounts[4];
            const allowedMintAccount2 = instruction2.accounts[4];
            expect(allowedMintAccount1.address).not.toBe(allowedMintAccount2.address);

            // Verify different mints produce different instance ATAs
            const instanceAtaAccount1 = instruction1.accounts[6];
            const instanceAtaAccount2 = instruction2.accounts[6];
            expect(instanceAtaAccount1.address).not.toBe(instanceAtaAccount2.address);
        });
    });
});
