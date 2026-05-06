import fs from "fs";
import path from "path";
import { preserveConfigFiles } from "./lib/utils";
import { createContraSwapCodamaBuilder } from "./lib/contra-swap-codama-builder";
import { renderVisitor as renderRustVisitor } from "@codama/renderers-rust";
import { renderVisitor as renderJavaScriptVisitor } from "@codama/renderers-js";

const projectRoot = path.join(__dirname, "..");
const idlDir = path.join(projectRoot, "idl");
const contraSwapIdl = JSON.parse(
  fs.readFileSync(path.join(idlDir, "contra_swap_program.json"), "utf-8"),
);
const rustClientsDir = path.join(__dirname, "..", "clients", "rust");
const typescriptClientsDir = path.join(
  __dirname,
  "..",
  "clients",
  "typescript",
);

const contraSwapCodama = createContraSwapCodamaBuilder(contraSwapIdl)
  .setInstructionAccountDefaultValues()
  .build();

const configPreserver = preserveConfigFiles(
  typescriptClientsDir,
  rustClientsDir,
);

contraSwapCodama.accept(
  renderRustVisitor(path.join(rustClientsDir, "src", "generated"), {
    formatCode: true,
    crateFolder: rustClientsDir,
    deleteFolderBeforeRendering: true,
  }),
);

contraSwapCodama.accept(
  renderJavaScriptVisitor(path.join(typescriptClientsDir, "src", "generated"), {
    formatCode: true,
    deleteFolderBeforeRendering: true,
  }),
);

configPreserver.restore();
