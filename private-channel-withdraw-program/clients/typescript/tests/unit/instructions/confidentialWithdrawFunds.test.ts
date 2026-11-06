import { expect } from '@jest/globals';
import {
    CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR,
    getConfidentialWithdrawFundsInstruction,
    getConfidentialWithdrawFundsInstructionAsync,
    getConfidentialWithdrawFundsInstructionDataCodec,
    getWithdrawFundsInstruction,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
} from '../../../src/generated';
import { mockTransactionSigner, TEST_ADDRESSES } from '../../setup/mocks';
import { getAddressEncoder, getProgramDerivedAddress } from '@solana/kit';

const input = (destination: typeof TEST_ADDRESSES.MINT | null) => ({
    user: mockTransactionSigner(TEST_ADDRESSES.WALLET),
    mint: TEST_ADDRESSES.MINT,
    tokenAccount: TEST_ADDRESSES.WALLET,
    withdrawConfig: TEST_ADDRESSES.WITHDRAW_CONFIG,
    treasuryTokenAccount: TEST_ADDRESSES.TREASURY_TOKEN_ACCOUNT,
    amount: 1000000n,
    destination,
});

describe('confidential_withdraw_funds', () => {
    it('should encode discriminator 2, amount and destination', () => {
        const instruction = getConfidentialWithdrawFundsInstruction(input(TEST_ADDRESSES.MINT));
        const decoded = getConfidentialWithdrawFundsInstructionDataCodec().decode(instruction.data);

        expect(CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR).toBe(2);
        expect(decoded.discriminator).toBe(CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR);
        expect(decoded.amount).toBe(1000000n);
        expect(decoded.destination).toEqual({ __option: 'Some', value: TEST_ADDRESSES.MINT });
    });

    // The indexer and gateway read both withdrawals with one decoder.
    it('should match WithdrawFunds apart from the discriminator', () => {
        for (const destination of [TEST_ADDRESSES.MINT, null]) {
            const confidential = getConfidentialWithdrawFundsInstruction(input(destination));
            const pub = getWithdrawFundsInstruction(input(destination));

            expect(confidential.programAddress).toBe(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS);
            const metas = (ix: typeof pub) => ix.accounts.map(({ address, role }) => ({ address, role }));
            expect(metas(confidential)).toEqual(metas(pub));
            expect(confidential.data.slice(1)).toEqual(pub.data.slice(1));
            expect(confidential.data[0]).toBe(2);
            expect(pub.data[0]).toBe(0);
        }
    });

    it('should derive withdrawConfig from the mint when not provided', async () => {
        const [expectedWithdrawConfig] = await getProgramDerivedAddress({
            programAddress: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_PROGRAM_ADDRESS,
            seeds: ['withdraw_config', getAddressEncoder().encode(TEST_ADDRESSES.MINT)],
        });
        const { withdrawConfig: _, ...rest } = input(null);

        const instruction = await getConfidentialWithdrawFundsInstructionAsync(rest);

        expect(instruction.accounts[5].address).toBe(expectedWithdrawConfig);
    });
});
