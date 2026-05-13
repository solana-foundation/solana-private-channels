import {
  Codama,
  publicKeyValueNode,
  setInstructionAccountDefaultValuesVisitor,
} from "codama";

const SYSTEM_PROGRAM_ID = "11111111111111111111111111111111";
const TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

export function setInstructionAccountDefaultValues(
  contraSwapCodama: Codama,
): Codama {
  contraSwapCodama.update(
    setInstructionAccountDefaultValuesVisitor([
      {
        account: "systemProgram",
        defaultValue: publicKeyValueNode(SYSTEM_PROGRAM_ID),
      },
      {
        account: "tokenProgram",
        defaultValue: publicKeyValueNode(TOKEN_PROGRAM_ID),
      },
      {
        account: "associatedTokenProgram",
        defaultValue: publicKeyValueNode(ATA_PROGRAM_ID),
      },
    ]),
  );
  return contraSwapCodama;
}
