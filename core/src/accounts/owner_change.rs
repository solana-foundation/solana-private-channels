use {
    super::types::StoredTransaction,
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::SanitizedTransaction},
    solana_svm::transaction_processing_result::ProcessedTransaction,
};

/// One recorded `AccountOwner` handoff, destined for `token_account_owner_change`.
///
/// Both sides are kept: the chain of rows for an address reconstructs its whole
/// ownership timeline, and a link that doesn't join tells a reader a handoff went
/// unrecorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerChangeRow {
    pub address: Vec<u8>,
    pub slot: i64,
    /// Position of the transaction in its block. Signature order is not
    /// execution order, so this is what puts two handoffs in one slot in the
    /// order they actually ran.
    pub tx_index: i32,
    pub signature: Vec<u8>,
    pub prev_owner: Vec<u8>,
    pub new_owner: Vec<u8>,
}

/// Metadata key holding the first slot `token_account_owner_change` is known to
/// cover. Readers must not scope history below it: the rows simply were not
/// being written yet, which is indistinguishable from an account that never
/// changed hands.
pub const OWNER_CHANGE_INDEXED_FROM_KEY: &str = "owner_change_indexed_from_slot";

/// Token-2022, which shares SPL Token's `SetAuthority` encoding for the four
/// authority types they have in common.
///
/// Recorded even though the ingress allowlist does not admit it yet: the
/// gateway already scopes history for both token programs, and a detector that
/// covered fewer would hand a later owner the whole history the moment the
/// allowlist gained one line. See `every_admitted_token_program_is_detected`.
const SPL_TOKEN_2022_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

/// SPL Token `SetAuthority` discriminator.
const SET_AUTHORITY_DISCRIMINATOR: u8 = 6;

/// `AuthorityType::AccountOwner`, the only variant that moves a token account
/// between wallets. The other three retarget mint or close authorities and leave
/// the `owner` field alone.
const AUTHORITY_TYPE_ACCOUNT_OWNER: u8 = 2;

/// `COption::Some` tag. `AccountOwner` rejects `None`, so any landed handoff
/// carries a pubkey.
const COPTION_SOME: u8 = 1;

/// Smallest `SetAuthority` payload that names a new authority.
const SET_AUTHORITY_WITH_PUBKEY_LEN: usize = 35;

/// Bulk-insert into `token_account_owner_change` inside an active PG tx.
pub(crate) async fn upsert_owner_change_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[OwnerChangeRow],
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let addresses: Vec<&[u8]> = rows.iter().map(|row| row.address.as_slice()).collect();
    let slots: Vec<i64> = rows.iter().map(|row| row.slot).collect();
    let tx_indexes: Vec<i32> = rows.iter().map(|row| row.tx_index).collect();
    let signatures: Vec<&[u8]> = rows.iter().map(|row| row.signature.as_slice()).collect();
    let prev_owners: Vec<&[u8]> = rows.iter().map(|row| row.prev_owner.as_slice()).collect();
    let new_owners: Vec<&[u8]> = rows.iter().map(|row| row.new_owner.as_slice()).collect();
    sqlx::query(
        "INSERT INTO token_account_owner_change
             (address, slot, tx_index, signature, prev_owner, new_owner)
         SELECT * FROM UNNEST(
             $1::bytea[], $2::int8[], $3::int4[], $4::bytea[], $5::bytea[], $6::bytea[])
         ON CONFLICT DO NOTHING",
    )
    .bind(&addresses)
    .bind(&slots)
    .bind(&tx_indexes)
    .bind(&signatures)
    .bind(&prev_owners)
    .bind(&new_owners)
    .execute(&mut **tx)
    .await
    .map(|_| ())
}

/// Rows for a transaction the executor just processed. Nothing is recorded for a
/// transaction that failed: the handoff never happened, and flooring an account
/// on a failed attempt would deny its owner history for no reason.
pub fn rows_from_processed(
    transaction: &SanitizedTransaction,
    processed: &ProcessedTransaction,
    slot: u64,
    tx_index: i32,
    signature: &Signature,
) -> Vec<OwnerChangeRow> {
    let executed = match processed {
        ProcessedTransaction::Executed(executed) => executed,
        // Loaded but never executed, so no instruction ran.
        ProcessedTransaction::FeesOnly(_) => return Vec::new(),
    };
    if executed.execution_details.status.is_err() {
        return Vec::new();
    }

    let message = transaction.message();
    let account_keys: Vec<Pubkey> = message.account_keys().iter().copied().collect();
    detect_owner_changes(
        &account_keys,
        message
            .program_instructions_iter()
            .map(|(program_id, instruction)| {
                (
                    program_id,
                    instruction.accounts.as_slice(),
                    instruction.data.as_slice(),
                )
            }),
        // A slot is never negative, and the column it lands in is a BIGINT.
        slot as i64,
        tx_index,
        signature,
    )
}

/// Rows re-derived from a transaction already written to the ledger, for the
/// repair pass. Address lookup tables are not admitted, so the static keys are
/// the whole key set.
pub fn rows_from_stored(
    stored: &StoredTransaction,
    slot: i64,
    tx_index: i32,
    signature: &Signature,
) -> Vec<OwnerChangeRow> {
    if stored.meta.err.is_some() {
        return Vec::new();
    }

    let message = &stored.transaction.message;
    let account_keys = message.static_account_keys();
    detect_owner_changes(
        account_keys,
        message.instructions().iter().filter_map(|instruction| {
            let program_id = account_keys.get(instruction.program_id_index as usize)?;
            Some((
                program_id,
                instruction.accounts.as_slice(),
                instruction.data.as_slice(),
            ))
        }),
        slot,
        tx_index,
        signature,
    )
}

/// Scan a landed transaction's top-level instructions for handoffs.
///
/// `SetAuthority` payload, per `TokenInstruction::pack`:
///
/// ```text
/// byte  0      : discriminator = 6
/// byte  1      : authority_type (2 = AccountOwner)
/// byte  2      : new_authority COption tag: 0 = None, 1 = Some
/// bytes 3..35  : new_authority (present only when tag = 1)
/// ```
///
/// Accounts are `[target, current_authority, ..multisig_signers]`. The program
/// validates the signer against the token account's `owner` field, so account 1
/// is that field's value going into the instruction — the previous owner, with
/// no pre-state lookup needed.
///
/// Inner instructions are not scanned: CPI recording is off, so they are not
/// available here, and no admitted program CPIs `SetAuthority` today. A handoff
/// that did slip through one would leave no row, so the gateway does not take an
/// absent row as proof on its own: it checks the address still derives from its
/// owner, which no CPI can fake.
fn detect_owner_changes<'a, I>(
    account_keys: &[Pubkey],
    instructions: I,
    slot: i64,
    tx_index: i32,
    signature: &Signature,
) -> Vec<OwnerChangeRow>
where
    I: Iterator<Item = (&'a Pubkey, &'a [u8], &'a [u8])>,
{
    let mut rows: Vec<OwnerChangeRow> = Vec::new();

    for (program_id, instruction_accounts, data) in instructions {
        if *program_id != spl_token::id() && *program_id != SPL_TOKEN_2022_PROGRAM_ID {
            continue;
        }
        if data.len() < SET_AUTHORITY_WITH_PUBKEY_LEN
            || data[0] != SET_AUTHORITY_DISCRIMINATOR
            || data[1] != AUTHORITY_TYPE_ACCOUNT_OWNER
            || data[2] != COPTION_SOME
        {
            continue;
        }

        let Ok(new_owner) = Pubkey::try_from(&data[3..SET_AUTHORITY_WITH_PUBKEY_LEN]) else {
            continue;
        };
        let Some(address) = instruction_accounts
            .first()
            .and_then(|index| account_keys.get(*index as usize))
        else {
            continue;
        };
        let Some(prev_owner) = instruction_accounts
            .get(1)
            .and_then(|index| account_keys.get(*index as usize))
        else {
            continue;
        };

        // A transaction may hand the same account on more than once. Only the net
        // move matters: the whole slot is excluded from every owner's window, so
        // the states it passed through in between are never readable anyway.
        match rows
            .iter_mut()
            .find(|row| row.address == address.to_bytes())
        {
            Some(existing) => existing.new_owner = new_owner.to_bytes().to_vec(),
            None => rows.push(OwnerChangeRow {
                address: address.to_bytes().to_vec(),
                slot,
                tx_index,
                signature: signature.as_ref().to_vec(),
                prev_owner: prev_owner.to_bytes().to_vec(),
                new_owner: new_owner.to_bytes().to_vec(),
            }),
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::instruction::Instruction;

    /// Build the instruction through spl-token itself, so the account order and
    /// payload this detector reads are the ones the program actually defines.
    fn set_authority_instruction(
        address: &Pubkey,
        current_owner: &Pubkey,
        new_owner: Option<&Pubkey>,
        authority_type: spl_token::instruction::AuthorityType,
    ) -> Instruction {
        spl_token::instruction::set_authority(
            &spl_token::id(),
            address,
            new_owner,
            authority_type,
            current_owner,
            &[],
        )
        .expect("set_authority builds")
    }

    /// Run the detector over one instruction, mirroring how a message resolves
    /// program ids and account indices.
    fn rows_for(instruction: &Instruction, slot: i64) -> Vec<OwnerChangeRow> {
        let mut account_keys: Vec<Pubkey> = instruction
            .accounts
            .iter()
            .map(|meta| meta.pubkey)
            .collect();
        account_keys.push(instruction.program_id);
        let instruction_accounts: Vec<u8> = (0..instruction.accounts.len() as u8).collect();
        let signature = Signature::default();

        detect_owner_changes(
            &account_keys,
            std::iter::once((
                &instruction.program_id,
                instruction_accounts.as_slice(),
                instruction.data.as_slice(),
            )),
            slot,
            0,
            &signature,
        )
    }

    #[test]
    fn records_both_sides_of_an_account_owner_handoff() {
        let address = Pubkey::new_unique();
        let current_owner = Pubkey::new_unique();
        let new_owner = Pubkey::new_unique();
        let slot = 4242;

        let rows = rows_for(
            &set_authority_instruction(
                &address,
                &current_owner,
                Some(&new_owner),
                spl_token::instruction::AuthorityType::AccountOwner,
            ),
            slot,
        );

        assert_eq!(
            rows,
            vec![OwnerChangeRow {
                address: address.to_bytes().to_vec(),
                slot,
                tx_index: 0,
                signature: Signature::default().as_ref().to_vec(),
                prev_owner: current_owner.to_bytes().to_vec(),
                new_owner: new_owner.to_bytes().to_vec(),
            }]
        );
    }

    /// The other authority types retarget a mint or a close authority and leave
    /// the `owner` field alone, so they must not floor anyone's history.
    #[test]
    fn ignores_authority_types_that_do_not_move_the_account() {
        for authority_type in [
            spl_token::instruction::AuthorityType::MintTokens,
            spl_token::instruction::AuthorityType::FreezeAccount,
            spl_token::instruction::AuthorityType::CloseAccount,
        ] {
            let rows = rows_for(
                &set_authority_instruction(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    Some(&Pubkey::new_unique()),
                    authority_type.clone(),
                ),
                1,
            );
            assert!(rows.is_empty(), "{authority_type:?} must not record a row");
        }
    }

    /// The gateway scopes a token account's history to the slots its caller
    /// owned it for, and can only do that from what this detector records. So
    /// every token program the ingress allowlist admits has to be one the
    /// detector reads: admitting one it does not would let a handoff go
    /// unrecorded, and the next owner would read the whole history unscoped.
    #[test]
    fn every_admitted_token_program_is_detected() {
        let address = Pubkey::new_unique();
        let current_owner = Pubkey::new_unique();
        let new_owner = Pubkey::new_unique();
        let handoff = set_authority_instruction(
            &address,
            &current_owner,
            Some(&new_owner),
            spl_token::instruction::AuthorityType::AccountOwner,
        );

        for program_id in [spl_token::id(), SPL_TOKEN_2022_PROGRAM_ID] {
            if !crate::transactions::is_allowed_program_instruction(&program_id, &handoff.data) {
                continue;
            }

            let rows = detect_owner_changes(
                &[address, current_owner],
                std::iter::once((&program_id, [0u8, 1].as_slice(), handoff.data.as_slice())),
                1,
                0,
                &Signature::default(),
            );
            assert_eq!(
                rows.len(),
                1,
                "{program_id} is admitted at ingress but its handoffs are not recorded"
            );
        }
    }

    #[test]
    fn ignores_another_program() {
        let mut instruction = set_authority_instruction(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            Some(&Pubkey::new_unique()),
            spl_token::instruction::AuthorityType::AccountOwner,
        );
        instruction.program_id = Pubkey::new_unique();

        assert!(rows_for(&instruction, 1).is_empty());
    }

    /// Two handoffs on one account collapse to the net move, so the row still
    /// links to the next change recorded for that address.
    #[test]
    fn collapses_repeated_handoffs_on_one_account_to_the_net_move() {
        let address = Pubkey::new_unique();
        let first_owner = Pubkey::new_unique();
        let second_owner = Pubkey::new_unique();
        let third_owner = Pubkey::new_unique();

        let account_keys = vec![address, first_owner, second_owner, third_owner];
        let first = set_authority_instruction(
            &address,
            &first_owner,
            Some(&second_owner),
            spl_token::instruction::AuthorityType::AccountOwner,
        );
        let second = set_authority_instruction(
            &address,
            &second_owner,
            Some(&third_owner),
            spl_token::instruction::AuthorityType::AccountOwner,
        );
        let program_id = spl_token::id();

        let rows = detect_owner_changes(
            &account_keys,
            [
                (&program_id, [0u8, 1].as_slice(), first.data.as_slice()),
                (&program_id, [0u8, 2].as_slice(), second.data.as_slice()),
            ]
            .into_iter(),
            7,
            0,
            &Signature::default(),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].prev_owner, first_owner.to_bytes().to_vec());
        assert_eq!(rows[0].new_owner, third_owner.to_bytes().to_vec());
    }

    #[test]
    fn ignores_truncated_payloads() {
        let instruction = set_authority_instruction(
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            Some(&Pubkey::new_unique()),
            spl_token::instruction::AuthorityType::AccountOwner,
        );
        let program_id = spl_token::id();

        for truncate_to in 0..SET_AUTHORITY_WITH_PUBKEY_LEN {
            let rows = detect_owner_changes(
                &[Pubkey::new_unique(), Pubkey::new_unique()],
                std::iter::once((
                    &program_id,
                    [0u8, 1].as_slice(),
                    &instruction.data[..truncate_to],
                )),
                1,
                0,
                &Signature::default(),
            );
            assert!(
                rows.is_empty(),
                "payload of {truncate_to} bytes recorded a row"
            );
        }
    }
}
