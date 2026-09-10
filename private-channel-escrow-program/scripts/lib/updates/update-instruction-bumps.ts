import { Codama, updateInstructionsVisitor, accountBumpValueNode } from 'codama';

export function updateInstructionBumps(privateChannelEscrowCodama: Codama): Codama {
    privateChannelEscrowCodama.update(
        updateInstructionsVisitor({
            createInstance: {
                arguments: {
                    bump: {
                        defaultValue: accountBumpValueNode('instance'),
                    },
                    bitmapBump: {
                        defaultValue: accountBumpValueNode('withdrawalBitmap'),
                    },
                },
            },
            allowMint: {
                arguments: {
                    bump: {
                        defaultValue: accountBumpValueNode('allowedMint'),
                    },
                },
            },
            addOperator: {
                arguments: {
                    bump: {
                        defaultValue: accountBumpValueNode('operatorPda'),
                    },
                },
            },
        }),
    );
    return privateChannelEscrowCodama;
}
