import {
    Codama,
    pdaNode,
    pdaValueNode,
    pdaSeedValueNode,
    publicKeyTypeNode,
    accountValueNode,
    variablePdaSeedNode,
    publicKeyValueNode,
    pdaLinkNode,
    setInstructionAccountDefaultValuesVisitor,
} from 'codama';

const PRIVATE_CHANNEL_ESCROW_PROGRAM_ID = '8msahYFvfeiiz3C2NzAorhfhAes5GaEThzmLWSLUHNkK';
const ATA_PROGRAM_ID = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL';
const SYSTEM_PROGRAM_ID = '11111111111111111111111111111111';
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const EVENT_AUTHORITY_PDA = '8WtLfmC99djequwMiicZij6p6t1hsyhUDqSSwJoAvJEK';

function createAtaPdaValueNode(ownerAccount: string, mintAccount: string, tokenProgram: string) {
    return pdaValueNode(
        pdaNode({
            name: 'associatedTokenAccount',
            seeds: [
                variablePdaSeedNode('owner', publicKeyTypeNode()),
                variablePdaSeedNode('tokenProgram', publicKeyTypeNode()),
                variablePdaSeedNode('mint', publicKeyTypeNode()),
            ],
            programId: ATA_PROGRAM_ID,
        }),
        [
            pdaSeedValueNode('owner', accountValueNode(ownerAccount)),
            pdaSeedValueNode('tokenProgram', accountValueNode(tokenProgram)),
            pdaSeedValueNode('mint', accountValueNode(mintAccount)),
        ],
    );
}

export function setInstructionAccountDefaultValues(privateChannelEscrowCodama: Codama): Codama {
    privateChannelEscrowCodama.update(
        setInstructionAccountDefaultValuesVisitor([
            // Global Constants
            {
                account: 'privateChannelEscrowProgram',
                defaultValue: publicKeyValueNode(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID),
            },
            {
                account: 'systemProgram',
                defaultValue: publicKeyValueNode(SYSTEM_PROGRAM_ID),
            },
            {
                account: 'tokenProgram',
                defaultValue: publicKeyValueNode(TOKEN_PROGRAM_ID),
            },
            {
                account: 'associatedTokenProgram',
                defaultValue: publicKeyValueNode(ATA_PROGRAM_ID),
            },
            {
                account: 'eventAuthority',
                defaultValue: publicKeyValueNode(EVENT_AUTHORITY_PDA),
            },
            {
                account: 'instanceAta',
                defaultValue: createAtaPdaValueNode('instance', 'mint', 'tokenProgram'),
            },
            {
                account: 'allowedMint',
                defaultValue: pdaValueNode(pdaLinkNode('allowedMint'), [
                    pdaSeedValueNode('instance', accountValueNode('instance')),
                    pdaSeedValueNode('mint', accountValueNode('mint')),
                ]),
            },
            {
                account: 'operatorPda',
                defaultValue: pdaValueNode(pdaLinkNode('operator'), [
                    pdaSeedValueNode('instance', accountValueNode('instance')),
                    pdaSeedValueNode('wallet', accountValueNode('operator')),
                ]),
            },
            {
                account: 'withdrawalBitmap',
                defaultValue: pdaValueNode(pdaLinkNode('withdrawalBitmap'), [
                    pdaSeedValueNode('instance', accountValueNode('instance')),
                ]),
            },

            // CreateInstance instruction
            {
                account: 'instance',
                defaultValue: pdaValueNode(pdaLinkNode('instance'), [
                    pdaSeedValueNode('instanceSeed', accountValueNode('instanceSeed')),
                ]),
            },

            // Deposit instruction
            {
                account: 'userAta',
                defaultValue: createAtaPdaValueNode('user', 'mint', 'tokenProgram'),
            },
        ]),
    );
    return privateChannelEscrowCodama;
}
