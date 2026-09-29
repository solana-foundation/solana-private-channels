import {
    accountValueNode,
    Codama,
    constantPdaSeedNode,
    pdaNode,
    pdaSeedValueNode,
    pdaValueNode,
    publicKeyTypeNode,
    publicKeyValueNode,
    setInstructionAccountDefaultValuesVisitor,
    stringTypeNode,
    stringValueNode,
    variablePdaSeedNode,
} from 'codama';

const WITHDRAW_PROGRAM_ID = 'J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi';
const ATA_PROGRAM_ID = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL';
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const SYSTEM_PROGRAM_ID = '11111111111111111111111111111111';

function createWithdrawConfigPdaValueNode(mintAccount: string) {
    return pdaValueNode(
        pdaNode({
            name: 'withdrawConfig',
            seeds: [
                constantPdaSeedNode(stringTypeNode('utf8'), stringValueNode('withdraw_config')),
                variablePdaSeedNode('mint', publicKeyTypeNode()),
            ],
            programId: WITHDRAW_PROGRAM_ID,
        }),
        [pdaSeedValueNode('mint', accountValueNode(mintAccount))],
    );
}

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

export function setInstructionAccountDefaultValues(privateChannelWithdrawCodama: Codama): Codama {
    privateChannelWithdrawCodama.update(
        setInstructionAccountDefaultValuesVisitor([
            {
                account: 'withdrawProgram',
                defaultValue: publicKeyValueNode(WITHDRAW_PROGRAM_ID),
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
                account: 'tokenAccount',
                defaultValue: createAtaPdaValueNode('user', 'mint', 'tokenProgram'),
            },
            {
                account: 'systemProgram',
                defaultValue: publicKeyValueNode(SYSTEM_PROGRAM_ID),
            },
            {
                account: 'withdrawConfig',
                defaultValue: createWithdrawConfigPdaValueNode('mint'),
            },
        ]),
    );
    return privateChannelWithdrawCodama;
}
