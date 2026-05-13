import { Codama, createFromJson } from "codama";
import { setInstructionAccountDefaultValues } from "./updates";

export class DvpSwapCodamaBuilder {
  private codama: Codama;

  constructor(dvpSwapIdl: unknown) {
    const idlJson =
      typeof dvpSwapIdl === "string" ? dvpSwapIdl : JSON.stringify(dvpSwapIdl);
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

export function createDvpSwapCodamaBuilder(
  dvpSwapIdl: unknown,
): DvpSwapCodamaBuilder {
  return new DvpSwapCodamaBuilder(dvpSwapIdl);
}
