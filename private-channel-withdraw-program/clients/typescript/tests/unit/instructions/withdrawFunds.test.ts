import { expect } from '@jest/globals';
import {
    getWithdrawFundsInstruction,
    getWithdrawFundsInstructionAsync,
    getWithdrawFundsInstructionDataCodec,
    WITHDRAW_FUNDS_DISCRIMINATOR,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';
import { AccountRole, getAddressEncoder, getProgramDerivedAddress } from '@solana/kit';
import { TOKEN_PROGRAM_ADDRESS, ASSOCIATED_TOKEN_PROGRAM_ADDRESS } from '@solana-program/token';

describe('withdraw_funds', () => {
    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator, amount, and destination', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const testAmount = 1000000n; // 1 USDC (6 decimals)
            const testDestination = TEST_ADDRESSES.MINT;

            const instruction = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: testAmount,
                destination: testDestination,
            });

            const decodedData = getWithdrawFundsInstructionDataCodec().decode(instruction.data);

            // Verify discriminator matches WITHDRAW_FUNDS_DISCRIMINATOR
            expect(decodedData.discriminator).toBe(WITHDRAW_FUNDS_DISCRIMINATOR);

            // Verify amount is correctly encoded as u64
            expect(decodedData.amount).toBe(testAmount);

            // Verify destination is correctly encoded as Option<Address>
            expect(decodedData.destination).toEqual({ __option: 'Some', value: testDestination });
        });

        it('should handle amount parameter correctly (u64)', async () => {
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
                const instruction = getWithdrawFundsInstruction({
                    user,
                    mint: TEST_ADDRESSES.MINT,
                    tokenAccount: TEST_ADDRESSES.WALLET,
                    withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                    treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                    amount: testAmount,
                    destination: null,
                });

                const decodedData = getWithdrawFundsInstructionDataCodec().decode(instruction.data);
                expect(decodedData.amount).toBe(testAmount);
                expect(typeof decodedData.amount).toBe('bigint');
            }

            // Test number input (should be converted to bigint)
            const numberAmount = 5000000; // 5 USDC
            const instruction = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: numberAmount,
                destination: null,
            });

            const decodedData = getWithdrawFundsInstructionDataCodec().decode(instruction.data);
            expect(decodedData.amount).toBe(BigInt(numberAmount));
        });

        it('should handle optional destination parameter (Some/None)', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const testAmount = 1000000n;

            // Test with None/null destination
            const instructionWithNullDestination = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: testAmount,
                destination: null,
            });

            const decodedDataNull = getWithdrawFundsInstructionDataCodec().decode(instructionWithNullDestination.data);
            expect(decodedDataNull.destination).toEqual({ __option: 'None' });

            // Test with Some destination
            const testDestination = TEST_ADDRESSES.WALLET;
            const instructionWithDestination = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: testAmount,
                destination: testDestination,
            });

            const decodedDataSome = getWithdrawFundsInstructionDataCodec().decode(instructionWithDestination.data);
            expect(decodedDataSome.destination).toEqual({ __option: 'Some', value: testDestination });

            // Test with different valid destination addresses
            const testDestinations = [TEST_ADDRESSES.WALLET, TEST_ADDRESSES.MINT];

            for (const destination of testDestinations) {
                const instruction = getWithdrawFundsInstruction({
                    user,
                    mint: TEST_ADDRESSES.MINT,
                    tokenAccount: TEST_ADDRESSES.WALLET,
                    withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                    treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                    amount: testAmount,
                    destination,
                });

                const decodedData = getWithdrawFundsInstructionDataCodec().decode(instruction.data);
                expect(decodedData.destination).toEqual({ __option: 'Some', value: destination });
            }
        });

        it('should decode instruction data correctly', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);
            const testAmount = 2500000n; // 2.5 USDC (6 decimals)
            const testDestination = TEST_ADDRESSES.WALLET;

            // Create instruction with specific data
            const instruction = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: testAmount,
                destination: testDestination,
            });

            // Decode the instruction data
            const decodedData = getWithdrawFundsInstructionDataCodec().decode(instruction.data);

            // Verify all fields are decoded correctly
            expect(decodedData.discriminator).toBe(WITHDRAW_FUNDS_DISCRIMINATOR);
            expect(decodedData.amount).toBe(testAmount);
            expect(decodedData.destination).toEqual({ __option: 'Some', value: testDestination });

            // Verify data types
            expect(typeof decodedData.discriminator).toBe('number');
            expect(typeof decodedData.amount).toBe('bigint');
            expect(typeof decodedData.destination).toBe('object');

            // Test with null destination
            const instructionNullDestination = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: testAmount,
                destination: null,
            });

            const decodedDataNull = getWithdrawFundsInstructionDataCodec().decode(instructionNullDestination.data);
            expect(decodedDataNull.destination).toEqual({ __option: 'None' });

            // Re-encode and verify it matches original
            const reEncodedData = getWithdrawFundsInstructionDataCodec().encode({
                amount: testAmount,
                destination: testDestination,
            });
            expect(reEncodedData).toEqual(instruction.data);
            const reEncodedDataNull = getWithdrawFundsInstructionDataCodec().encode({
                amount: testAmount,
                destination: null,
            });
            expect(reEncodedDataNull).toEqual(instructionNullDestination.data);
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: user, mint, tokenAccount, tokenProgram, associatedTokenProgram, withdrawFeeConfig, treasuryTokenAccount', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const instruction = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: 1000000n,
                destination: null,
            });

            // Based on withdraw_funds.rs, should have 7 accounts
            expect(instruction.accounts).toHaveLength(7);

            // Account 0: user (Signer)
            const userAccount = instruction.accounts[0];
            expect(userAccount.address).toBe(TEST_ADDRESSES.WALLET);

            // Account 1: mint (Readonly)
            const mintAccount = instruction.accounts[1];
            expect(mintAccount.address).toBe(TEST_ADDRESSES.MINT);

            // Account 2: tokenAccount (Writable)
            const tokenAccountAccount = instruction.accounts[2];
            expect(tokenAccountAccount.address).toBe(TEST_ADDRESSES.WALLET);

            // Account 3: tokenProgram (Readonly)
            const tokenProgramAccount = instruction.accounts[3];
            expect(tokenProgramAccount.address).toBe(TOKEN_PROGRAM_ADDRESS);

            // Account 4: associatedTokenProgram (Readonly)
            const associatedTokenProgramAccount = instruction.accounts[4];
            expect(associatedTokenProgramAccount.address).toBe(ASSOCIATED_TOKEN_PROGRAM_ADDRESS);

            // Account 5: withdrawFeeConfig (Readonly)
            const withdrawFeeConfigAccount = instruction.accounts[5];
            expect(withdrawFeeConfigAccount.address).toBe(TEST_ADDRESSES.WITHDRAW_FEE_CONFIG);

            // Account 6: treasuryTokenAccount (Writable)
            const treasuryTokenAccountAccount = instruction.accounts[6];
            expect(treasuryTokenAccountAccount.address).toBe(TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const instruction = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: 1000000n,
                destination: null,
            });

            // Account 0: user - should be Signer
            const userAccount = instruction.accounts[0];
            expect(userAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 1: mint - should be Writable
            const mintAccount = instruction.accounts[1];
            expect(mintAccount.role).toBe(AccountRole.WRITABLE);

            // Account 2: tokenAccount - should be Writable
            const tokenAccountAccount = instruction.accounts[2];
            expect(tokenAccountAccount.role).toBe(AccountRole.WRITABLE);

            // Account 3: tokenProgram - should be Readonly
            const tokenProgramAccount = instruction.accounts[3];
            expect(tokenProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 4: associatedTokenProgram - should be Readonly
            const associatedTokenProgramAccount = instruction.accounts[4];
            expect(associatedTokenProgramAccount.role).toBe(AccountRole.READONLY);

            // Account 5: withdrawFeeConfig - should be Readonly
            const withdrawFeeConfigAccount = instruction.accounts[5];
            expect(withdrawFeeConfigAccount.role).toBe(AccountRole.READONLY);

            // Account 6: treasuryTokenAccount - should be Writable, it receives the fee
            const treasuryTokenAccountAccount = instruction.accounts[6];
            expect(treasuryTokenAccountAccount.role).toBe(AccountRole.WRITABLE);
        });

        it('should use correct program addresses', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const instruction = getWithdrawFundsInstruction({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                withdrawFeeConfig: TEST_ADDRESSES.WITHDRAW_FEE_CONFIG,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: 1000000n,
                destination: null,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS);

            // Verify tokenProgram uses the correct address
            const tokenProgramAccount = instruction.accounts[3];
            expect(tokenProgramAccount.address).toBe(TOKEN_PROGRAM_ADDRESS);

            // Verify associatedTokenProgram uses the correct address
            const associatedTokenProgramAccount = instruction.accounts[4];
            expect(associatedTokenProgramAccount.address).toBe(ASSOCIATED_TOKEN_PROGRAM_ADDRESS);
        });

        it('should derive withdrawFeeConfig from the mint when not provided', async () => {
            const user = mockTransactionSigner(TEST_ADDRESSES.WALLET);

            const [expectedWithdrawFeeConfig] = await getProgramDerivedAddress({
                programAddress: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
                seeds: ['withdraw_fee_config', getAddressEncoder().encode(TEST_ADDRESSES.MINT)],
            });

            const instruction = await getWithdrawFundsInstructionAsync({
                user,
                mint: TEST_ADDRESSES.MINT,
                tokenAccount: TEST_ADDRESSES.WALLET,
                treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
                amount: 1000000n,
                destination: null,
            });

            expect(instruction.accounts[5].address).toBe(expectedWithdrawFeeConfig);
        });
    });
});
