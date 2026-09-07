#![no_std]

pub mod constants;
pub mod error;
pub mod events;
pub mod instructions;
pub mod processor;
pub mod state;

#[cfg(not(feature = "no-entrypoint"))]
pub mod entrypoint;

use pinocchio::address::declare_id;
declare_id!("9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU");
