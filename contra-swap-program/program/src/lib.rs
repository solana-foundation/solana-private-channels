#![no_std]

pub mod discriminator;
pub mod error;
// pub mod events;
pub mod instructions;
pub mod processor;
pub mod state;

#[cfg(not(feature = "no-entrypoint"))]
pub mod entrypoint;

use pinocchio::address::declare_id;
declare_id!("DzG1qJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC");
