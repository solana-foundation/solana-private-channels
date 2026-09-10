import {
    Codama,
    constantPdaSeedNode,
    stringTypeNode,
    stringValueNode,
    variablePdaSeedNode,
    publicKeyTypeNode,
    addPdasVisitor,
} from 'codama';

export function appendPdaDerivers(privateChannelEscrowCodama: Codama): Codama {
    privateChannelEscrowCodama.update(
        addPdasVisitor({
            privateChannelEscrowProgram: [
                {
                    name: 'instance',
                    seeds: [
                        constantPdaSeedNode(stringTypeNode('utf8'), stringValueNode('instance')),
                        variablePdaSeedNode('instanceSeed', publicKeyTypeNode()),
                    ],
                },
                {
                    name: 'allowedMint',
                    seeds: [
                        constantPdaSeedNode(stringTypeNode('utf8'), stringValueNode('allowed_mint')),
                        variablePdaSeedNode('instance', publicKeyTypeNode()),
                        variablePdaSeedNode('mint', publicKeyTypeNode()),
                    ],
                },
                {
                    name: 'operator',
                    seeds: [
                        constantPdaSeedNode(stringTypeNode('utf8'), stringValueNode('operator')),
                        variablePdaSeedNode('instance', publicKeyTypeNode()),
                        variablePdaSeedNode('wallet', publicKeyTypeNode()),
                    ],
                },
                {
                    name: 'eventAuthority',
                    seeds: [constantPdaSeedNode(stringTypeNode('utf8'), stringValueNode('event_authority'))],
                },
                {
                    name: 'withdrawalBitmap',
                    seeds: [
                        constantPdaSeedNode(stringTypeNode('utf8'), stringValueNode('withdrawal_bitmap')),
                        variablePdaSeedNode('instance', publicKeyTypeNode()),
                    ],
                },
            ],
        }),
    );
    return privateChannelEscrowCodama;
}
