#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContraSwapInstructionDiscriminators {
    CreateDvp = 0,
    ReclaimDvp = 1,
    SettleDvp = 2,
    CancelDvp = 3,
    RejectDvp = 4,
}

impl TryFrom<u8> for ContraSwapInstructionDiscriminators {
    type Error = ();

    fn try_from(discriminator: u8) -> Result<Self, Self::Error> {
        match discriminator {
            0 => Ok(Self::CreateDvp),
            1 => Ok(Self::ReclaimDvp),
            2 => Ok(Self::SettleDvp),
            3 => Ok(Self::CancelDvp),
            4 => Ok(Self::RejectDvp),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discriminator_all_valid() {
        let cases = [
            (0u8, ContraSwapInstructionDiscriminators::CreateDvp),
            (1u8, ContraSwapInstructionDiscriminators::ReclaimDvp),
            (2u8, ContraSwapInstructionDiscriminators::SettleDvp),
            (3u8, ContraSwapInstructionDiscriminators::CancelDvp),
            (4u8, ContraSwapInstructionDiscriminators::RejectDvp),
        ];

        for (byte, expected) in cases {
            let result = ContraSwapInstructionDiscriminators::try_from(byte);
            assert_eq!(result, Ok(expected), "byte {byte}");
        }
    }

    #[test]
    fn test_discriminator_invalid() {
        let result = ContraSwapInstructionDiscriminators::try_from(5u8);

        assert!(result.is_err());
    }
}
