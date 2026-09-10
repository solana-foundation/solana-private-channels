extern crate alloc;

use alloc::vec::Vec;

pub trait Discriminator {
    const DISCRIMINATOR: u8;
}

#[repr(u8)]
pub enum PrivateChannelEscrowAccountDiscriminators {
    InstanceDiscriminator = 0,
    OperatorDiscriminator = 1,
    AllowedMintDiscriminator = 2,
    WithdrawalBitmapDiscriminator = 3,
}

#[repr(u8)]
pub enum PrivateChannelEscrowInstructionDiscriminators {
    CreateInstance = 0,
    AllowMint = 1,
    BlockMint = 2,
    AddOperator = 3,
    RemoveOperator = 4,
    SetNewAdmin = 5,
    Deposit = 6,
    ReleaseFunds = 7,
    RotateBitmap = 8,
    EmitEvent = 228,
}

impl TryFrom<u8> for PrivateChannelEscrowInstructionDiscriminators {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::CreateInstance),
            1 => Ok(Self::AllowMint),
            2 => Ok(Self::BlockMint),
            3 => Ok(Self::AddOperator),
            4 => Ok(Self::RemoveOperator),
            5 => Ok(Self::SetNewAdmin),
            6 => Ok(Self::Deposit),
            7 => Ok(Self::ReleaseFunds),
            8 => Ok(Self::RotateBitmap),
            228 => Ok(Self::EmitEvent),
            _ => Err(()),
        }
    }
}

pub trait AccountSerialize: Discriminator {
    fn to_bytes_inner(&self) -> Vec<u8>;

    fn to_bytes(&self) -> Vec<u8> {
        let inner = self.to_bytes_inner();
        let mut data = Vec::with_capacity(1 + inner.len());
        data.push(Self::DISCRIMINATOR);
        data.extend_from_slice(&inner);
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // All 10 defined instruction discriminators must parse successfully.
    // If a variant is added or removed, this test will catch the gap.
    #[test]
    fn test_instruction_discriminator_all_valid_values() {
        for byte in [0u8, 1, 2, 3, 4, 5, 6, 7, 8, 228] {
            assert!(
                PrivateChannelEscrowInstructionDiscriminators::try_from(byte).is_ok(),
                "byte {byte} should be a valid discriminator"
            );
        }
    }

    // Bytes that don't map to any instruction must return Err.
    // The entrypoint relies on this to reject malformed instructions before dispatch.
    #[test]
    fn test_instruction_discriminator_invalid_values() {
        for byte in [9u8, 10, 100, 227, 229, 255] {
            assert!(
                PrivateChannelEscrowInstructionDiscriminators::try_from(byte).is_err(),
                "byte {byte} should not map to a valid discriminator"
            );
        }
    }
}
