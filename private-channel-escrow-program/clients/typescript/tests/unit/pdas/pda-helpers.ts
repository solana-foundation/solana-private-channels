import { PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS } from '../../../src/generated/programs/privateChannelEscrowProgram';
import {
    getAddressEncoder,
    getProgramDerivedAddress,
    getUtf8Encoder,
    type Address,
    type ProgramDerivedAddress,
} from '@solana/kit';

// PDA seed constants
const INSTANCE_PDA_SEED = 'instance';
const ALLOWED_MINT_PDA_SEED = 'allowed_mint';
const OPERATOR_PDA_SEED = 'operator';
const EVENT_AUTHORITY_PDA_SEED = 'event_authority';
const WITHDRAWAL_BITMAP_PDA_SEED = 'withdrawal_bitmap';

export async function expectedInstancePda(instanceSeed: Address): Promise<ProgramDerivedAddress> {
    return await getProgramDerivedAddress({
        programAddress: PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
        seeds: [getUtf8Encoder().encode(INSTANCE_PDA_SEED), getAddressEncoder().encode(instanceSeed)],
    });
}

export async function expectedAllowedMintPda(instance: Address, mint: Address): Promise<ProgramDerivedAddress> {
    return await getProgramDerivedAddress({
        programAddress: PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
        seeds: [
            getUtf8Encoder().encode(ALLOWED_MINT_PDA_SEED),
            getAddressEncoder().encode(instance),
            getAddressEncoder().encode(mint),
        ],
    });
}

export async function expectedOperatorPda(instance: Address, wallet: Address): Promise<ProgramDerivedAddress> {
    return await getProgramDerivedAddress({
        programAddress: PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
        seeds: [
            getUtf8Encoder().encode(OPERATOR_PDA_SEED),
            getAddressEncoder().encode(instance),
            getAddressEncoder().encode(wallet),
        ],
    });
}

export async function expectedWithdrawalBitmapPda(instance: Address): Promise<ProgramDerivedAddress> {
    return await getProgramDerivedAddress({
        programAddress: PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
        seeds: [getUtf8Encoder().encode(WITHDRAWAL_BITMAP_PDA_SEED), getAddressEncoder().encode(instance)],
    });
}

export async function expectedEventAuthorityPda(): Promise<ProgramDerivedAddress> {
    return await getProgramDerivedAddress({
        programAddress: PRIVATE_CHANNEL_ESCROW_PROGRAM_PROGRAM_ADDRESS,
        seeds: [getUtf8Encoder().encode(EVENT_AUTHORITY_PDA_SEED)],
    });
}
