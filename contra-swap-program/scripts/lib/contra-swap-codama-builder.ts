import { Codama, createFromJson } from "codama";
import { setInstructionAccountDefaultValues } from "./updates";

export class ContraSwapCodamaBuilder {
  private codama: Codama;

  constructor(contraSwapIdl: unknown) {
    const idlJson =
      typeof contraSwapIdl === "string"
        ? contraSwapIdl
        : JSON.stringify(contraSwapIdl);
    this.codama = createFromJson(idlJson);
  }

  setInstructionAccountDefaultValues(): this {
    this.codama = setInstructionAccountDefaultValues(this.codama);
    return this;
  }

  build(): Codama {
    return this.codama;
  }
}

export function createContraSwapCodamaBuilder(
  contraSwapIdl: unknown,
): ContraSwapCodamaBuilder {
  return new ContraSwapCodamaBuilder(contraSwapIdl);
}
