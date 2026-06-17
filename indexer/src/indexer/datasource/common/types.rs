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
}

#[derive(Debug, Deserialize, Clone)]
pub struct CompiledInstruction {
    #[serde(rename = "programIdIndex")]
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String, // base58 encoded
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
