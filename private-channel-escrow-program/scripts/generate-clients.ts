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

// Emit to src/generated to match the Rust client and the @private-channel-escrow Vite/tsconfig alias; syncPackageJson off since the client isn't published.
privateChannelEscrowCodama.accept(
    renderJavaScriptVisitor(path.join(typescriptClientsDir, 'src', 'generated'), {
        formatCode: true,
        // Default appends another src/generated under this path; '.' writes in place to avoid double-nesting.
        generatedFolder: '.',
        syncPackageJson: false,
        deleteFolderBeforeRendering: true,
    }),
);

// Restore configuration files after generation
configPreserver.restore();
