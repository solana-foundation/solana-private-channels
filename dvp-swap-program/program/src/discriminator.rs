#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DvpSwapInstructionDiscriminators {
    CreateDvp = 0,
    ReclaimDvp = 1,
    SettleDvp = 2,
    CancelDvp = 3,
    RejectDvp = 4,
}

impl TryFrom<u8> for DvpSwapInstructionDiscriminators {
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
