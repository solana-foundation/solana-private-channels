import { expect } from '@jest/globals';
import { AccountRole } from '@solana/kit';
import {
    getBlockMintInstructionAsync,
    getBlockMintInstructionDataCodec,
    findAllowedMintPda,
    BLOCK_MINT_DISCRIMINATOR,
    PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES, EXPECTED_PROGRAM_ADDRESS } from '../../setup/mocks';

describe('blockMint', () => {
    describe('Instruction data validation', () => {
        it('should encode instruction data with correct discriminator (2)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            const decodedData = getBlockMintInstructionDataCodec().decode(instruction.data);

            // Verify discriminator is 2 as defined in the program (BlockMint = 2)
            expect(decodedData.discriminator).toBe(BLOCK_MINT_DISCRIMINATOR);
            expect(decodedData.discriminator).toBe(2);
        });

        it('should encode both gates independently', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            // Opposite values so a swapped or shared byte would fail here
            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                blockDeposits: true,
                blockWithdrawals: false,
            });

            const decodedData = getBlockMintInstructionDataCodec().decode(instruction.data);

            expect(Object.keys(decodedData)).toEqual(['discriminator', 'blockDeposits', 'blockWithdrawals']);
            expect(decodedData.blockDeposits).toBe(true);
            expect(decodedData.blockWithdrawals).toBe(false);

            // discriminator + one byte per gate
            expect(instruction.data.length).toBe(3);
        });
    });

    describe('Account requirements', () => {
        it('should include all required accounts: payer, admin, instance, mint, allowedMint, eventAuthority, privateChannelEscrowProgram', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            // Based on program instruction definition, BlockMint should have 7 accounts
            expect(instruction.accounts).toHaveLength(7);

            // Account 0: payer (WritableSigner)
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.address).toBe(TEST_ADDRESSES.PAYER);

            // Account 1: admin (ReadonlySigner)
            const adminAccount = instruction.accounts[1];
            expect(adminAccount.address).toBe(TEST_ADDRESSES.ADMIN);

            // Account 2: instance (Readonly PDA)
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.address).toBe(TEST_ADDRESSES.INSTANCE);

            // Account 3: mint (Readonly)
            const mintAccount = instruction.accounts[3];
            expect(mintAccount.address).toBe(TEST_ADDRESSES.WRAPPED_SOL);

            // Account 4: allowedMint (Writable PDA - auto-derived)
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBeDefined();

            // Account 5: eventAuthority (Readonly PDA)
            const eventAuthorityAccount = instruction.accounts[5];
            expect(eventAuthorityAccount.address).toBeDefined();

            // Account 6: privateChannelEscrowProgram (Readonly)
            const privateChannelEscrowProgramAccount = instruction.accounts[6];
            expect(privateChannelEscrowProgramAccount.address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });

        it('should set correct account permissions (writable/readable/signer)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            // Account 0: payer - should be WritableSigner
            const payerAccount = instruction.accounts[0];
            expect(payerAccount.role).toBe(AccountRole.WRITABLE_SIGNER);

            // Account 1: admin - should be ReadonlySigner
            const adminAccount = instruction.accounts[1];
            expect(adminAccount.role).toBe(AccountRole.READONLY_SIGNER);

            // Account 2: instance - should be Readonly (not writable for BlockMint)
            const instanceAccount = instruction.accounts[2];
            expect(instanceAccount.role).toBe(AccountRole.READONLY);

            // Account 3: mint - should be Readonly
            const mintAccount = instruction.accounts[3];
            expect(mintAccount.role).toBe(AccountRole.READONLY);

            // Account 4: allowedMint - should be Writable (PDA being modified)
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.role).toBe(AccountRole.WRITABLE);

            // Account 5: eventAuthority - should be Readonly (PDA, not a signer)
            const eventAuthorityAccount = instruction.accounts[5];
            expect(eventAuthorityAccount.role).toBe(AccountRole.READONLY);

            // Account 6: privateChannelEscrowProgram - should be Readonly
            const privateChannelEscrowProgramAccount = instruction.accounts[6];
            expect(privateChannelEscrowProgramAccount.role).toBe(AccountRole.READONLY);
        });

        it('should use correct program address', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            // Verify the instruction uses the correct program address
            expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
            expect(instruction.programAddress).toBe(EXPECTED_PROGRAM_ADDRESS);
            expect(instruction.accounts[6].address).toBe(PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS);
        });
    });

    describe('Automatic allowedMint PDA derivation', () => {
        it('should automatically derive allowedMint PDA when not provided', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            // Get expected allowedMint PDA using findAllowedMintPda
            const [expectedAllowedMintPda] = await findAllowedMintPda({
                instance,
                mint,
            });

            // Generate instruction without providing allowedMint - should be auto-derived
            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                blockDeposits: true,
                blockWithdrawals: true,
                // Not providing allowedMint - should be auto-derived from instance + mint
            });

            // Verify the automatically derived allowedMint PDA matches expected address
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBe(expectedAllowedMintPda);
        });

        it('should use provided allowedMint address when supplied (override auto-derivation)', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
            const instance = TEST_ADDRESSES.INSTANCE;
            const mint = TEST_ADDRESSES.WRAPPED_SOL;

            // Use a custom allowedMint address (different mint but valid PDA)
            const [customAllowedMintPda] = await findAllowedMintPda({
                instance,
                mint: TEST_ADDRESSES.USDC_MINT,
            });

            const instruction = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance,
                mint,
                allowedMint: customAllowedMintPda,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            // Verify the provided address is used instead of auto-derived one
            const allowedMintAccount = instruction.accounts[4];
            expect(allowedMintAccount.address).toBe(customAllowedMintPda);

            // Ensure it's not the auto-derived one for the actual mint
            const [autoDerivadPda] = await findAllowedMintPda({
                instance,
                mint,
            });
            expect(allowedMintAccount.address).not.toBe(autoDerivadPda);
        });

        it('should derive different PDAs for different instance/mint combinations', async () => {
            const payer = mockTransactionSigner(TEST_ADDRESSES.PAYER);
            const admin = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

            // Test with different instance/mint combinations
            const instance1 = TEST_ADDRESSES.INSTANCE;
            const instance2 = TEST_ADDRESSES.ALLOWED_MINT; // Using as another address
            const mint1 = TEST_ADDRESSES.WRAPPED_SOL;
            const mint2 = TEST_ADDRESSES.USDC_MINT;

            // Same instance, different mints
            const instruction1 = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance: instance1,
                mint: mint1,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            const instruction2 = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance: instance1,
                mint: mint2,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            // Different instance, same mint
            const instruction3 = await getBlockMintInstructionAsync({
                payer,
                admin,
                instance: instance2,
                mint: mint1,
                blockDeposits: true,
                blockWithdrawals: true,
            });

            // Get the allowedMint accounts from each instruction
            const allowedMint1 = instruction1.accounts[4].address;
            const allowedMint2 = instruction2.accounts[4].address;
            const allowedMint3 = instruction3.accounts[4].address;

            // All should be different from each other
            expect(allowedMint1).not.toBe(allowedMint2);
            expect(allowedMint1).not.toBe(allowedMint3);
            expect(allowedMint2).not.toBe(allowedMint3);

            // Verify they match expected PDAs
            const [expectedPda1] = await findAllowedMintPda({
                instance: instance1,
                mint: mint1,
            });
            const [expectedPda2] = await findAllowedMintPda({
                instance: instance1,
                mint: mint2,
            });
            const [expectedPda3] = await findAllowedMintPda({
                instance: instance2,
                mint: mint1,
            });

            expect(allowedMint1).toBe(expectedPda1);
            expect(allowedMint2).toBe(expectedPda2);
            expect(allowedMint3).toBe(expectedPda3);
        });
    });
});
