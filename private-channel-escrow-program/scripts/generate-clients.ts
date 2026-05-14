import fs from 'fs';
import path from 'path';
import { preserveConfigFiles } from './lib/utils';
import { createPrivateChannelEscrowCodamaBuilder } from './lib/private-channel-escrow-codama-builder';
import { renderVisitor as renderRustVisitor } from '@codama/renderers-rust';
import { renderVisitor as renderJavaScriptVisitor } from '@codama/renderers-js';

const projectRoot = path.join(__dirname, '..');
const idlDir = path.join(projectRoot, 'idl');
const privateChannelEscrowIdl = JSON.parse(
    fs.readFileSync(path.join(idlDir, 'private_channel_escrow_program.json'), 'utf-8'),
);
const rustClientsDir = path.join(__dirname, '..', 'clients', 'rust');
const typescriptClientsDir = path.join(__dirname, '..', 'clients', 'typescript');

// Create and configure the codama instance using the builder pattern
const privateChannelEscrowCodama = createPrivateChannelEscrowCodamaBuilder(privateChannelEscrowIdl)
    .appendAccountDiscriminator()
    .appendPdaDerivers()
    .setInstructionAccountDefaultValues()
    .updateInstructionBumps()
    .removeEmitInstruction()
    .build();

// Preserve configuration files during generation
const configPreserver = preserveConfigFiles(typescriptClientsDir, rustClientsDir);

// Generate Rust client
privateChannelEscrowCodama.accept(
    renderRustVisitor(path.join(rustClientsDir, 'src', 'generated'), {
        formatCode: true,
        crateFolder: rustClientsDir,
        deleteFolderBeforeRendering: true,
    }),
);

// Generate TypeScript client
privateChannelEscrowCodama.accept(
    renderJavaScriptVisitor(path.join(typescriptClientsDir, 'src', 'generated'), {
        formatCode: true,
        deleteFolderBeforeRendering: true,
    }),
);

// Restore configuration files after generation
configPreserver.restore();
