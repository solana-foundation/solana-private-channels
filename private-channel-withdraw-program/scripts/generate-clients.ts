import fs from 'fs';
import path from 'path';
import { preserveConfigFiles } from './lib/utils';
import { createPrivateChannelWithdrawCodamaBuilder } from './lib/private-channel-withdraw-codama-builder';
import { renderVisitor as renderRustVisitor } from '@codama/renderers-rust';
import { renderVisitor as renderJavaScriptVisitor } from '@codama/renderers-js';

const projectRoot = path.join(__dirname, '..');
const idlDir = path.join(projectRoot, 'idl');
const privateChannelWithdrawIdl = JSON.parse(
    fs.readFileSync(path.join(idlDir, 'private_channel_withdraw_program.json'), 'utf-8'),
);
const rustClientsDir = path.join(__dirname, '..', 'clients', 'rust');
const typescriptClientsDir = path.join(__dirname, '..', 'clients', 'typescript');

// Create and configure the codama instance using the builder pattern
const privateChannelWithdrawCodama = createPrivateChannelWithdrawCodamaBuilder(privateChannelWithdrawIdl)
    .setInstructionAccountDefaultValues()
    .build();

// Preserve configuration files during generation
const configPreserver = preserveConfigFiles(typescriptClientsDir, rustClientsDir);

// Generate Rust client
privateChannelWithdrawCodama.accept(
    renderRustVisitor(path.join(rustClientsDir, 'src', 'generated'), {
        formatCode: true,
        crateFolder: rustClientsDir,
        deleteFolderBeforeRendering: true,
    }),
);

// Generate TypeScript client (renderers-js v2 signature: first arg is the
// package root, generatedFolder option points at the codegen subdir).
// syncPackageJson is disabled because we don't publish the generated client
// as a standalone npm package — admin-ui consumes it via Vite path alias.
privateChannelWithdrawCodama.accept(
    renderJavaScriptVisitor(typescriptClientsDir, {
        formatCode: true,
        generatedFolder: 'src/generated',
        syncPackageJson: false,
        deleteFolderBeforeRendering: true,
    }),
);

// Restore configuration files after generation
configPreserver.restore();
