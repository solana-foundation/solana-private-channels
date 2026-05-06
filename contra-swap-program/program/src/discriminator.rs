#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContraSwapInstructionDiscriminators {
    CreateDvp = 0,
    FundDvp = 1,
    ReclaimDvp = 2,
    SettleDvp = 3,
    CancelDvp = 4,
    RejectDvp = 5,
}

impl TryFrom<u8> for ContraSwapInstructionDiscriminators {
    type Error = ();

    fn try_from(discriminator: u8) -> Result<Self, Self::Error> {
        match discriminator {
            0 => Ok(Self::CreateDvp),
            1 => Ok(Self::FundDvp),
            2 => Ok(Self::ReclaimDvp),
            3 => Ok(Self::SettleDvp),
            4 => Ok(Self::CancelDvp),
            5 => Ok(Self::RejectDvp),
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
            (1u8, ContraSwapInstructionDiscriminators::FundDvp),
            (2u8, ContraSwapInstructionDiscriminators::ReclaimDvp),
            (3u8, ContraSwapInstructionDiscriminators::SettleDvp),
            (4u8, ContraSwapInstructionDiscriminators::CancelDvp),
            (5u8, ContraSwapInstructionDiscriminators::RejectDvp),
        ];

        for (byte, expected) in cases {
            let result = ContraSwapInstructionDiscriminators::try_from(byte);
            assert_eq!(result, Ok(expected), "byte {byte}");
        }
    }

    #[test]
    fn test_discriminator_invalid() {
        let result = ContraSwapInstructionDiscriminators::try_from(6u8);

        assert!(result.is_err());
    }
}
