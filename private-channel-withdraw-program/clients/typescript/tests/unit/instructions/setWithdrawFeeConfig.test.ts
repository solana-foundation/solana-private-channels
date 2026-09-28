import { expect } from '@jest/globals';
import {
    getSetWithdrawFeeConfigInstructionAsync,
    SET_WITHDRAW_FEE_CONFIG_DISCRIMINATOR,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';
import { AccountRole, getAddressEncoder, getProgramDerivedAddress, getU64Encoder } from '@solana/kit';

const SYSTEM_PROGRAM_ADDRESS = '11111111111111111111111111111111';

describe('set_withdraw_fee_config', () => {
    // The program parses these bytes by hand: discriminator, fee (u64 LE), treasury.
    it('should encode discriminator, fee and treasury in the order the program parses them', async () => {
        const authority = mockTransactionSigner(TEST_ADDRESSES.ADMIN);
        const fee = 1_234_567n;

        const instruction = await getSetWithdrawFeeConfigInstructionAsync({
            authority,
            mint: TEST_ADDRESSES.MINT,
            fee,
            treasury: TEST_ADDRESSES.ADMIN,
        });

        expect(instruction.data[0]).toBe(SET_WITHDRAW_FEE_CONFIG_DISCRIMINATOR);
        expect(instruction.data[0]).toBe(1);
        expect(Array.from(instruction.data.slice(1, 9))).toEqual(Array.from(getU64Encoder().encode(fee)));
        expect(Array.from(instruction.data.slice(9, 41))).toEqual(
            Array.from(getAddressEncoder().encode(TEST_ADDRESSES.ADMIN)),
        );
        expect(instruction.data).toHaveLength(41);
    });

    it('should derive withdrawFeeConfig from the mint and default the system program', async () => {
        const authority = mockTransactionSigner(TEST_ADDRESSES.ADMIN);

        const [expectedWithdrawFeeConfig] = await getProgramDerivedAddress({
            programAddress: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
            seeds: ['withdraw_fee_config', getAddressEncoder().encode(TEST_ADDRESSES.MINT)],
        });

        const instruction = await getSetWithdrawFeeConfigInstructionAsync({
            authority,
            mint: TEST_ADDRESSES.MINT,
            fee: 1000n,
            treasury: TEST_ADDRESSES.ADMIN,
        });

        expect(instruction.programAddress).toBe(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS);
        expect(instruction.accounts).toHaveLength(4);

        // Account 0: authority - the mint authority, which also pays for the config
        expect(instruction.accounts[0].address).toBe(TEST_ADDRESSES.ADMIN);
        expect(instruction.accounts[0].role).toBe(AccountRole.WRITABLE_SIGNER);

        // Account 1: mint - Readonly
        expect(instruction.accounts[1].address).toBe(TEST_ADDRESSES.MINT);
        expect(instruction.accounts[1].role).toBe(AccountRole.READONLY);

        // Account 2: withdrawFeeConfig - Writable PDA
        expect(instruction.accounts[2].address).toBe(expectedWithdrawFeeConfig);
        expect(instruction.accounts[2].role).toBe(AccountRole.WRITABLE);

        // Account 3: systemProgram - Readonly
        expect(instruction.accounts[3].address).toBe(SYSTEM_PROGRAM_ADDRESS);
        expect(instruction.accounts[3].role).toBe(AccountRole.READONLY);
    });
});
