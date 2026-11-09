//! Channel admin rotation: move MintTokens and FreezeAccount of every receipt mint
//! from the old admin to the new one. Pure decisions live here; the bin does the I/O.

use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
use spl_token_2022::{
    extension::{
        permanent_delegate::PermanentDelegate, BaseStateWithExtensions, StateWithExtensions,
    },
    instruction::{set_authority, AuthorityType},
    state::Mint,
};

/// A receipt mint as read from the channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelMint {
    pub mint: Pubkey,
    pub program_id: Pubkey,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub permanent_delegate: Option<Pubkey>,
}

fn is_token_program(program_id: &Pubkey) -> bool {
    *program_id == spl_token::ID || *program_id == spl_token_2022::ID
}

/// Decodes a mint account owned by `owner`, which must be spl-token or Token-2022.
pub fn decode_mint(mint: Pubkey, owner: Pubkey, data: &[u8]) -> Result<ChannelMint, String> {
    if !is_token_program(&owner) {
        return Err(format!(
            "mint {mint} is owned by {owner}, not a token program"
        ));
    }
    // A plain spl-token mint is a Token-2022 base with no extensions.
    let state = StateWithExtensions::<Mint>::unpack(data)
        .map_err(|e| format!("mint {mint} does not decode: {e}"))?;
    let permanent_delegate = state
        .get_extension::<PermanentDelegate>()
        .ok()
        .and_then(|ext| Option::<Pubkey>::from(ext.delegate));
    Ok(ChannelMint {
        mint,
        program_id: owner,
        mint_authority: state.base.mint_authority.into(),
        freeze_authority: state.base.freeze_authority.into(),
        permanent_delegate,
    })
}

/// Refuses when the old admin is the permanent delegate of any mint or the configured one.
pub fn refuse_delegate<'a>(
    old_admin: &Pubkey,
    delegates: impl IntoIterator<Item = &'a Pubkey>,
) -> Result<(), String> {
    if delegates.into_iter().any(|d| d == old_admin) {
        return Err(format!(
            "old admin {old_admin} is the permanent delegate; rotating it would leave seize \
             power with the retired key"
        ));
    }
    Ok(())
}

/// No listed mint usually means the wrong indexer DB, so it is refused unless allowed.
pub fn refuse_empty(listed: usize, allow_empty: bool) -> Result<(), String> {
    if listed == 0 && !allow_empty {
        return Err(
            "the indexer DB lists no mint; check the DB URL, or pass --allow-empty \
             when the channel really has none"
                .to_string(),
        );
    }
    Ok(())
}

/// `SetAuthority` for each MintTokens or FreezeAccount authority still held by old.
/// The SetAuthority builder refuses a mint under any other program.
pub fn rotation_instructions(
    mints: &[ChannelMint],
    old_admin: &Pubkey,
    new_admin: &Pubkey,
) -> Result<Vec<Instruction>, String> {
    let mut instructions = Vec::new();
    for m in mints {
        let held = [
            (m.mint_authority, AuthorityType::MintTokens),
            (m.freeze_authority, AuthorityType::FreezeAccount),
        ];
        for (authority, authority_type) in held {
            if authority != Some(*old_admin) {
                continue;
            }
            let ix = set_authority(
                &m.program_id,
                &m.mint,
                Some(new_admin),
                authority_type,
                old_admin,
                &[],
            )
            .map_err(|e| format!("SetAuthority for {}: {e}", m.mint))?;
            instructions.push(ix);
        }
    }
    Ok(instructions)
}

/// Errors listing every mint that still has old as mint or freeze authority.
pub fn leftover_check(mints: &[ChannelMint], old_admin: &Pubkey) -> Result<(), String> {
    let left: Vec<String> = mints
        .iter()
        .filter(|m| m.mint_authority == Some(*old_admin) || m.freeze_authority == Some(*old_admin))
        .map(|m| m.mint.to_string())
        .collect();
    if left.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} mint(s) still have the old admin as an authority: {}",
            left.len(),
            left.join(", ")
        ))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::program_pack::Pack;
    use spl_token_2022::{
        extension::{BaseStateWithExtensionsMut, ExtensionType},
        state::Mint as T22Mint,
    };

    // SetAuthority is instruction 6; byte 1 is the authority type (MintTokens 0, FreezeAccount 1).
    const SET_AUTHORITY: u8 = 6;

    fn mint(program_id: Pubkey, mint_auth: Option<Pubkey>, freeze: Option<Pubkey>) -> ChannelMint {
        ChannelMint {
            mint: Pubkey::new_unique(),
            program_id,
            mint_authority: mint_auth,
            freeze_authority: freeze,
            permanent_delegate: None,
        }
    }

    /// (program, mint, authority type, current authority, new authority) per instruction.
    fn summarize(ixs: &[Instruction]) -> Vec<(Pubkey, Pubkey, u8, Pubkey, Pubkey)> {
        ixs.iter()
            .map(|ix| {
                assert_eq!(ix.data[0], SET_AUTHORITY);
                // COption tag 1 then the new authority.
                assert_eq!(ix.data[2], 1);
                let new = Pubkey::try_from(&ix.data[3..35]).unwrap();
                assert!(ix.accounts[1].is_signer);
                (
                    ix.program_id,
                    ix.accounts[0].pubkey,
                    ix.data[1],
                    ix.accounts[1].pubkey,
                    new,
                )
            })
            .collect()
    }

    #[test]
    fn builds_set_authority_for_exactly_the_authorities_held_by_old() {
        let old = Pubkey::new_unique();
        let new = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let t22_both = mint(spl_token_2022::ID, Some(old), Some(old));
        let spl_both = mint(spl_token::ID, Some(old), Some(old));
        let moved = mint(spl_token_2022::ID, Some(new), Some(new));
        let foreign = mint(spl_token_2022::ID, Some(other), Some(other));
        let mint_only = mint(spl_token_2022::ID, Some(old), Some(new));
        let freeze_only = mint(spl_token::ID, None, Some(old));
        let mints = vec![
            t22_both.clone(),
            moved,
            spl_both.clone(),
            foreign,
            mint_only.clone(),
            freeze_only.clone(),
        ];

        let ixs = rotation_instructions(&mints, &old, &new).unwrap();

        let t22 = spl_token_2022::ID;
        let spl = spl_token::ID;
        assert_eq!(
            summarize(&ixs),
            vec![
                (t22, t22_both.mint, 0, old, new),
                (t22, t22_both.mint, 1, old, new),
                (spl, spl_both.mint, 0, old, new),
                (spl, spl_both.mint, 1, old, new),
                (t22, mint_only.mint, 0, old, new),
                (spl, freeze_only.mint, 1, old, new),
            ]
        );
    }

    #[test]
    fn a_mint_under_an_unknown_program_is_refused() {
        let old = Pubkey::new_unique();
        let stray = mint(Pubkey::new_unique(), Some(old), Some(old));
        assert!(rotation_instructions(&[stray], &old, &Pubkey::new_unique()).is_err());
    }

    #[test]
    fn refuses_when_old_admin_is_the_permanent_delegate() {
        let old = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        assert!(refuse_delegate(&old, [&delegate]).is_ok());
        assert!(refuse_delegate(&old, [&delegate, &old]).is_err());
    }

    #[test]
    fn refuses_zero_listed_mints_unless_allowed() {
        assert!(refuse_empty(0, false).is_err());
        assert!(refuse_empty(0, true).is_ok());
        assert!(refuse_empty(1, false).is_ok());
    }

    #[test]
    fn leftover_check_errors_on_any_mint_still_held_by_old() {
        let old = Pubkey::new_unique();
        let new = Pubkey::new_unique();
        let done = mint(spl_token_2022::ID, Some(new), Some(new));
        let other = mint(spl_token_2022::ID, Some(Pubkey::new_unique()), None);
        assert!(leftover_check(&[done.clone(), other.clone()], &old).is_ok());

        let freeze_left = mint(spl_token_2022::ID, Some(new), Some(old));
        let err = leftover_check(&[done.clone(), freeze_left.clone()], &old).unwrap_err();
        assert!(err.contains(&freeze_left.mint.to_string()));

        let mint_left = mint(spl_token::ID, Some(old), Some(new));
        assert!(leftover_check(&[mint_left, done], &old).is_err());
    }

    #[test]
    fn decodes_authorities_and_delegate_of_both_programs() {
        let auth = Pubkey::new_unique();
        let freeze = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();
        let key = Pubkey::new_unique();

        let mut spl_data = vec![0u8; spl_token::state::Mint::LEN];
        spl_token::state::Mint {
            mint_authority: Some(auth).into(),
            supply: 0,
            decimals: 6,
            is_initialized: true,
            freeze_authority: None.into(),
        }
        .pack_into_slice(&mut spl_data);
        let spl = decode_mint(key, spl_token::ID, &spl_data).unwrap();
        assert_eq!(spl.program_id, spl_token::ID);
        assert_eq!(spl.mint_authority, Some(auth));
        assert_eq!(spl.freeze_authority, None);
        assert_eq!(spl.permanent_delegate, None);

        let len = ExtensionType::try_calculate_account_len::<T22Mint>(&[
            ExtensionType::PermanentDelegate,
        ])
        .unwrap();
        let mut t22_data = vec![0u8; len];
        let mut state =
            spl_token_2022::extension::StateWithExtensionsMut::<T22Mint>::unpack_uninitialized(
                &mut t22_data,
            )
            .unwrap();
        state
            .init_extension::<PermanentDelegate>(true)
            .unwrap()
            .delegate = Some(delegate).try_into().unwrap();
        state.base = T22Mint {
            mint_authority: Some(auth).into(),
            supply: 0,
            decimals: 6,
            is_initialized: true,
            freeze_authority: Some(freeze).into(),
        };
        state.pack_base();
        state.init_account_type().unwrap();
        let t22 = decode_mint(key, spl_token_2022::ID, &t22_data).unwrap();
        assert_eq!(t22.mint_authority, Some(auth));
        assert_eq!(t22.freeze_authority, Some(freeze));
        assert_eq!(t22.permanent_delegate, Some(delegate));

        assert!(decode_mint(key, Pubkey::new_unique(), &spl_data).is_err());
    }
}
