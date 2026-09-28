#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrivateChannelWithdrawInstructionDiscriminators {
    WithdrawFunds = 0,
    SetWithdrawFeeConfig = 1,
}

impl TryFrom<u8> for PrivateChannelWithdrawInstructionDiscriminators {
    type Error = ();

    fn try_from(discriminator: u8) -> Result<Self, Self::Error> {
        match discriminator {
            0 => Ok(Self::WithdrawFunds),
            1 => Ok(Self::SetWithdrawFeeConfig),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discriminator_valid() {
        let result = PrivateChannelWithdrawInstructionDiscriminators::try_from(0u8);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            PrivateChannelWithdrawInstructionDiscriminators::WithdrawFunds
        );
    }

    #[test]
    fn test_discriminator_invalid() {
        let result = PrivateChannelWithdrawInstructionDiscriminators::try_from(2u8);

        assert!(result.is_err());
    }
}
