use crate::{
    config::ProgramType,
    indexer::datasource::common::parser::{EscrowInstruction, WithdrawInstruction},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// Instruction with metadata (slot, program_type, signature, etc.)
#[derive(Debug, Clone)]
pub struct InstructionWithMetadata {
    pub instruction: ProgramInstruction,
    pub slot: u64,
    pub program_type: ProgramType,
    pub signature: Option<String>,
    /// Absolute position of this instruction within its transaction's instruction
    /// list. Paired with `signature` it forms the durable per-instruction identity,
    /// so multiple economic events sharing one signature stay distinct.
    pub instruction_index: u32,
    /// Position of this instruction within its parent's inner-instruction set when
    /// it is a CPI (cross-program invocation); `None` for a top-level instruction.
    /// Extends the identity to `(signature, instruction_index, inner_index)` so a
    /// foreign program CPI-ing into escrow/withdraw persists as its own row.
    pub inner_index: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CompiledInstruction {
    #[serde(rename = "programIdIndex")]
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String, // base58 encoded
}

/// Where an instruction sits in its transaction, used to find the self-CPI
/// event it emitted.
#[derive(Debug, Clone, Copy)]
pub struct InstructionLocation {
    /// Position of this instruction, or of its top-level ancestor when it is a CPI.
    pub top_level_index: u32,
    /// Set only when this instruction is itself an inner (CPI) instruction.
    pub inner: Option<InnerLocation>,
}

#[derive(Debug, Clone, Copy)]
pub struct InnerLocation {
    /// Position within the inner-instruction set of the top-level ancestor.
    pub inner_index: u32,
    /// CPI depth, used to bound the subtree of inner instructions this one owns.
    pub stack_height: Option<u32>,
}

impl InstructionLocation {
    /// A plain top-level instruction with no inner position.
    pub fn top_level(top_level_index: u32) -> Self {
        Self {
            top_level_index,
            inner: None,
        }
    }
}

/// Messages sent from datasources to transaction processor
#[derive(Debug, Clone)]
pub enum ProcessorMessage {
    /// An instruction to be processed
    Instruction(InstructionWithMetadata),
    /// Marks the completion of a slot (sent after all instructions from that slot)
    SlotComplete {
        slot: u64,
        program_type: ProgramType,
    },
}

// Channel types for sending/receiving messages to transaction processor
pub type InstructionSender = mpsc::Sender<ProcessorMessage>;
pub type InstructionReceiver = mpsc::Receiver<ProcessorMessage>;

/// Top-level instruction enum that dispatches to program-specific instructions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProgramInstruction {
    Escrow(Box<EscrowInstruction>),
    Withdraw(Box<WithdrawInstruction>),
}
