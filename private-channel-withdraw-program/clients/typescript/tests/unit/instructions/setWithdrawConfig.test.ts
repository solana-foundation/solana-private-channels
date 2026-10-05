import { expect } from '@jest/globals';
import {
    getSetWithdrawConfigInstructionAsync,
    SET_WITHDRAW_CONFIG_DISCRIMINATOR,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';
import { AccountRole, getAddressEncoder, getProgramDerivedAddress, getU64Encoder } from '@solana/kit';

const SYSTEM_PROGRAM_ADDRESS = '11111111111111111111111111111111';

describe('set_withdraw_config', () => {
    // The program parses these bytes by hand: discriminator, fee (u64 LE), AllowMint slot (u64 LE),
    // treasury, minimum withdraw amount (u64 LE).
    it('should encode discriminator, fee, AllowMint slot, treasury and minimum in the order the program parses them', async () => {
        const authority = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
        const fee = 1_234_567n;
        const allowMintSlot = 7_654_321n;
        const minWithdrawAmount = 2_468_024n;

        const instruction = await getSetWithdrawConfigInstructionAsync({
            authority,
            mint: TEST_ADDRESSES.MINT,
            fee,
            allowMintSlot,
            treasury: TEST_ADDRESSES.ADMIN,
            minWithdrawAmount,
        });

        expect(instruction.data[0]).toBe(SET_WITHDRAW_CONFIG_DISCRIMINATOR);
        expect(instruction.data[0]).toBe(1);
        expect(Array.from(instruction.data.slice(1, 9))).toEqual(Array.from(getU64Encoder().encode(fee)));
        expect(Array.from(instruction.data.slice(9, 17))).toEqual(Array.from(getU64Encoder().encode(allowMintSlot)));
        expect(Array.from(instruction.data.slice(17, 49))).toEqual(
            Array.from(getAddressEncoder().encode(TEST_ADDRESSES.ADMIN)),
        );
        expect(Array.from(instruction.data.slice(49, 57))).toEqual(
            Array.from(getU64Encoder().encode(minWithdrawAmount)),
        );
        expect(instruction.data).toHaveLength(57);
    });

    it('should derive withdrawConfig from the mint and default the system program', async () => {
        const authority = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

        const [expectedWithdrawConfig] = await getProgramDerivedAddress({
            programAddress: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
            seeds: ['withdraw_config', getAddressEncoder().encode(TEST_ADDRESSES.MINT)],
        });

        const instruction = await getSetWithdrawConfigInstructionAsync({
            authority,
            mint: TEST_ADDRESSES.MINT,
            fee: 1000n,
            allowMintSlot: 0n,
            treasury: TEST_ADDRESSES.ADMIN,
            minWithdrawAmount: 100n,
        });

        expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS);
        expect(instruction.accounts).toHaveLength(4);

        // Account 0: authority - the mint authority, which also pays for the config
        expect(instruction.accounts[0].address).toBe(TEST_ADDRESSES.ADMIN);
        expect(instruction.accounts[0].role).toBe(AccountRole.WRITABLE_SIGNER);

        // Account 1: mint - Readonly
        expect(instruction.accounts[1].address).toBe(TEST_ADDRESSES.MINT);
        expect(instruction.accounts[1].role).toBe(AccountRole.READONLY);

        // Account 2: withdrawConfig - Writable PDA
        expect(instruction.accounts[2].address).toBe(expectedWithdrawConfig);
        expect(instruction.accounts[2].role).toBe(AccountRole.WRITABLE);

        // Account 3: systemProgram - Readonly
        expect(instruction.accounts[3].address).toBe(SYSTEM_PROGRAM_ADDRESS);
        expect(instruction.accounts[3].role).toBe(AccountRole.READONLY);
    });
});
