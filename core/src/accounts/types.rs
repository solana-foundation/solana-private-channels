use {
    bincode::Options,
    serde::{Deserialize, Serialize},
    solana_account_decoder_client_types::token::UiTokenAmount,
    solana_sdk::{
        clock::UnixTimestamp,
        message::{compiled_instruction::CompiledInstruction, v0::LoadedAddresses},
        pubkey::Pubkey,
        transaction::{TransactionError, VersionedTransaction},
    },
    solana_transaction_context::transaction::TransactionReturnData,
    solana_transaction_status::{
        EncodeError, EncodedConfirmedTransactionWithStatusMeta, TransactionStatusMeta,
        TransactionTokenBalance, TransactionWithStatusMeta, UiTransactionEncoding,
        VersionedTransactionWithStatusMeta,
    },
    solana_transaction_status_client_types::{InnerInstruction, InnerInstructions, Reward},
};

/// Marks a stored transaction row, so a row from any other format is refused
/// rather than misread.
const ROW_MAGIC: [u8; 4] = *b"SPCT";

/// Layout of everything after the header. Bump it for any change to the layout.
const ROW_FORMAT_VERSION: u8 = 1;

const ROW_HEADER_LEN: usize = ROW_MAGIC.len() + 1;

/// Why a stored transaction row could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum StoredRowError {
    #[error("stored transaction row does not start with the expected format header")]
    UnknownFormat,
    #[error("stored transaction row format version {0} is not supported")]
    UnsupportedVersion(u8),
    #[error("stored transaction row is malformed: {0}")]
    Malformed(String),
    #[error("stored transaction bytes are malformed: {0}")]
    MalformedTransaction(String),
    #[error("transaction could not be encoded for storage: {0}")]
    Encode(String),
}

/// A settled transaction together with what executing it produced.
///
/// This is the in-memory form. The stored form, written by `to_bytes`, keeps the
/// transaction as the exact wire bytes its sender signed and owns every type in
/// the metadata, so an upstream release cannot change what is on disk.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredTransaction {
    pub slot: u64,
    pub block_time: UnixTimestamp,
    pub transaction: VersionedTransaction,
    pub meta: StoredTransactionMeta,
}

/// The layout written after the header, borrowing so a write copies nothing.
#[derive(Serialize)]
struct StoredRowRef<'a> {
    slot: u64,
    block_time: UnixTimestamp,
    transaction: &'a [u8],
    meta: &'a StoredTransactionMeta,
}

/// The layout read after the header.
#[derive(Deserialize)]
struct StoredRow {
    slot: u64,
    block_time: UnixTimestamp,
    transaction: Vec<u8>,
    meta: StoredTransactionMeta,
}

/// Execution metadata in the shape this crate stores it.
///
/// Every nested type is owned here, so only this crate can change the layout.
/// The two error enums are the exception: upstream pins their serialized layout
/// with frozen ABI digests, which is what a validator relies on to read its own
/// ledger, so they are stored as they are.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTransactionMeta {
    pub err: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<StoredInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<StoredTokenBalance>>,
    pub post_token_balances: Option<Vec<StoredTokenBalance>>,
    pub rewards: Option<Vec<StoredReward>>,
    pub loaded_addresses: StoredLoadedAddresses,
    pub return_data: Option<StoredReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

/// Inner instructions issued by one top level instruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredInnerInstructions {
    pub index: u8,
    pub instructions: Vec<StoredInstruction>,
}

/// One inner instruction, in compiled form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
}

/// A token balance snapshot taken before or after execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: StoredTokenAmount,
    pub owner: String,
    pub program_id: String,
}

/// A token amount together with the decimals needed to render it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTokenAmount {
    pub ui_amount: Option<f64>,
    pub decimals: u8,
    pub amount: String,
    pub ui_amount_string: String,
}

/// A reward credited to an account. The channel issues none, but the field is
/// kept so conversion to and from runtime metadata stays lossless.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredReward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: Option<StoredRewardType>,
    pub commission: Option<u8>,
    pub commission_bps: Option<u16>,
}

/// Why a reward was credited. Stored by variant index, so only ever append.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
    DeactivatedStake,
}

/// Addresses resolved from lookup tables, always empty because the channel
/// refuses lookup tables at admission.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredLoadedAddresses {
    pub writable: Vec<[u8; 32]>,
    pub readonly: Vec<[u8; 32]>,
}

/// Data a program returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredReturnData {
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
}

impl From<solana_reward_info::RewardType> for StoredRewardType {
    fn from(reward_type: solana_reward_info::RewardType) -> Self {
        use solana_reward_info::RewardType as Upstream;
        match reward_type {
            Upstream::Fee => Self::Fee,
            Upstream::Rent => Self::Rent,
            Upstream::Staking => Self::Staking,
            Upstream::Voting => Self::Voting,
            Upstream::DeactivatedStake => Self::DeactivatedStake,
        }
    }
}

impl From<StoredRewardType> for solana_reward_info::RewardType {
    fn from(reward_type: StoredRewardType) -> Self {
        match reward_type {
            StoredRewardType::Fee => Self::Fee,
            StoredRewardType::Rent => Self::Rent,
            StoredRewardType::Staking => Self::Staking,
            StoredRewardType::Voting => Self::Voting,
            StoredRewardType::DeactivatedStake => Self::DeactivatedStake,
        }
    }
}

impl From<UiTokenAmount> for StoredTokenAmount {
    fn from(amount: UiTokenAmount) -> Self {
        Self {
            ui_amount: amount.ui_amount,
            decimals: amount.decimals,
            amount: amount.amount,
            ui_amount_string: amount.ui_amount_string,
        }
    }
}

impl From<StoredTokenAmount> for UiTokenAmount {
    fn from(amount: StoredTokenAmount) -> Self {
        Self {
            ui_amount: amount.ui_amount,
            decimals: amount.decimals,
            amount: amount.amount,
            ui_amount_string: amount.ui_amount_string,
        }
    }
}

impl From<TransactionTokenBalance> for StoredTokenBalance {
    fn from(balance: TransactionTokenBalance) -> Self {
        Self {
            account_index: balance.account_index,
            mint: balance.mint,
            ui_token_amount: balance.ui_token_amount.into(),
            owner: balance.owner,
            program_id: balance.program_id,
        }
    }
}

impl From<StoredTokenBalance> for TransactionTokenBalance {
    fn from(balance: StoredTokenBalance) -> Self {
        Self {
            account_index: balance.account_index,
            mint: balance.mint,
            ui_token_amount: balance.ui_token_amount.into(),
            owner: balance.owner,
            program_id: balance.program_id,
        }
    }
}

impl From<Reward> for StoredReward {
    fn from(reward: Reward) -> Self {
        Self {
            pubkey: reward.pubkey,
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: reward.reward_type.map(Into::into),
            commission: reward.commission,
            commission_bps: reward.commission_bps,
        }
    }
}

impl From<StoredReward> for Reward {
    fn from(reward: StoredReward) -> Self {
        Self {
            pubkey: reward.pubkey,
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: reward.reward_type.map(Into::into),
            commission: reward.commission,
            commission_bps: reward.commission_bps,
        }
    }
}

impl From<TransactionStatusMeta> for StoredTransactionMeta {
    fn from(meta: TransactionStatusMeta) -> Self {
        Self {
            err: meta.status.err(),
            fee: meta.fee,
            pre_balances: meta.pre_balances,
            post_balances: meta.post_balances,
            inner_instructions: meta.inner_instructions.map(|groups| {
                groups
                    .into_iter()
                    .map(|group| StoredInnerInstructions {
                        index: group.index,
                        instructions: group
                            .instructions
                            .into_iter()
                            .map(|inner| StoredInstruction {
                                program_id_index: inner.instruction.program_id_index,
                                accounts: inner.instruction.accounts,
                                data: inner.instruction.data,
                                stack_height: inner.stack_height,
                            })
                            .collect(),
                    })
                    .collect()
            }),
            log_messages: meta.log_messages,
            pre_token_balances: meta
                .pre_token_balances
                .map(|balances| balances.into_iter().map(Into::into).collect()),
            post_token_balances: meta
                .post_token_balances
                .map(|balances| balances.into_iter().map(Into::into).collect()),
            rewards: meta
                .rewards
                .map(|rewards| rewards.into_iter().map(Into::into).collect()),
            loaded_addresses: StoredLoadedAddresses {
                writable: meta
                    .loaded_addresses
                    .writable
                    .iter()
                    .map(Pubkey::to_bytes)
                    .collect(),
                readonly: meta
                    .loaded_addresses
                    .readonly
                    .iter()
                    .map(Pubkey::to_bytes)
                    .collect(),
            },
            return_data: meta.return_data.map(|data| StoredReturnData {
                program_id: data.program_id.to_bytes(),
                data: data.data,
            }),
            compute_units_consumed: meta.compute_units_consumed,
            cost_units: meta.cost_units,
        }
    }
}

impl From<StoredTransactionMeta> for TransactionStatusMeta {
    fn from(meta: StoredTransactionMeta) -> Self {
        Self {
            status: meta.err.map_or(Ok(()), Err),
            fee: meta.fee,
            pre_balances: meta.pre_balances,
            post_balances: meta.post_balances,
            inner_instructions: meta.inner_instructions.map(|groups| {
                groups
                    .into_iter()
                    .map(|group| InnerInstructions {
                        index: group.index,
                        instructions: group
                            .instructions
                            .into_iter()
                            .map(|inner| InnerInstruction {
                                instruction: CompiledInstruction {
                                    program_id_index: inner.program_id_index,
                                    accounts: inner.accounts,
                                    data: inner.data,
                                },
                                stack_height: inner.stack_height,
                            })
                            .collect(),
                    })
                    .collect()
            }),
            log_messages: meta.log_messages,
            pre_token_balances: meta
                .pre_token_balances
                .map(|balances| balances.into_iter().map(Into::into).collect()),
            post_token_balances: meta
                .post_token_balances
                .map(|balances| balances.into_iter().map(Into::into).collect()),
            rewards: meta
                .rewards
                .map(|rewards| rewards.into_iter().map(Into::into).collect()),
            loaded_addresses: LoadedAddresses {
                writable: meta
                    .loaded_addresses
                    .writable
                    .into_iter()
                    .map(Pubkey::new_from_array)
                    .collect(),
                readonly: meta
                    .loaded_addresses
                    .readonly
                    .into_iter()
                    .map(Pubkey::new_from_array)
                    .collect(),
            },
            return_data: meta.return_data.map(|data| TransactionReturnData {
                program_id: Pubkey::new_from_array(data.program_id),
                data: data.data,
            }),
            compute_units_consumed: meta.compute_units_consumed,
            cost_units: meta.cost_units,
        }
    }
}

/// The encoding used after the header, for both directions.
fn row_codec() -> impl Options {
    bincode::options()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

impl StoredTransaction {
    /// Encode the row as it is written to Postgres and Redis.
    pub fn to_bytes(&self) -> Result<Vec<u8>, StoredRowError> {
        // Wincode produces the bytes the sender signed, which the protocol freezes.
        let transaction = wincode::serialize(&self.transaction)
            .map_err(|e| StoredRowError::Encode(e.to_string()))?;
        let row = StoredRowRef {
            slot: self.slot,
            block_time: self.block_time,
            transaction: &transaction,
            meta: &self.meta,
        };
        let mut bytes = Vec::with_capacity(ROW_HEADER_LEN + transaction.len() + 128);
        bytes.extend_from_slice(&ROW_MAGIC);
        bytes.push(ROW_FORMAT_VERSION);
        row_codec()
            .serialize_into(&mut bytes, &row)
            .map_err(|e| StoredRowError::Encode(e.to_string()))?;
        Ok(bytes)
    }

    /// Decode a row written by `to_bytes`, refusing any other format.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StoredRowError> {
        let Some((header, body)) = bytes.split_at_checked(ROW_HEADER_LEN) else {
            return Err(StoredRowError::UnknownFormat);
        };
        if header[..ROW_MAGIC.len()] != ROW_MAGIC {
            return Err(StoredRowError::UnknownFormat);
        }
        let version = header[ROW_MAGIC.len()];
        if version != ROW_FORMAT_VERSION {
            return Err(StoredRowError::UnsupportedVersion(version));
        }
        let row: StoredRow = row_codec()
            .deserialize(body)
            .map_err(|e| StoredRowError::Malformed(e.to_string()))?;
        let transaction = wincode::deserialize::<VersionedTransaction>(&row.transaction)
            .map_err(|e| StoredRowError::MalformedTransaction(e.to_string()))?;
        Ok(Self {
            slot: row.slot,
            block_time: row.block_time,
            transaction,
            meta: row.meta,
        })
    }

    pub fn transaction_with_status_meta(&self) -> TransactionWithStatusMeta {
        TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
            transaction: self.transaction.clone(),
            meta: self.meta.clone().into(),
        })
    }

    /// Encode one stored transaction for a `getTransaction` reply.
    ///
    /// The reply is assembled here rather than through the upstream wrapper
    /// because that wrapper always reports a position within the block, and a
    /// row is stored by signature alone with no position recorded. Reporting
    /// none keeps the field out of the response entirely, which is what callers
    /// already see, instead of inventing a zero that would read as the first
    /// transaction in the block.
    pub fn encoded_transaction(
        &self,
        encoding: &UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta, EncodeError> {
        let transaction = self.transaction_with_status_meta().encode(
            *encoding,
            max_supported_transaction_version,
            true,
        )?;
        Ok(EncodedConfirmedTransactionWithStatusMeta {
            slot: self.slot,
            transaction,
            block_time: Some(self.block_time),
            transaction_index: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::TransactionVersion;
    use solana_sdk::{
        hash::Hash,
        instruction::InstructionError,
        message::{v0, v1, Message, MessageHeader, VersionedMessage},
        signature::Signature,
    };

    /// Where the byte fixtures live, relative to the crate root.
    const FIXTURE_DIR: &str = "src/accounts/fixtures/stored_transaction";

    /// Offset of the transaction length inside a row: header, slot, block time.
    const TX_LEN_OFFSET: usize = ROW_HEADER_LEN + 8 + 8;

    fn header() -> MessageHeader {
        MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        }
    }

    fn instruction() -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data: vec![2, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0],
        }
    }

    fn keys() -> Vec<Pubkey> {
        vec![
            Pubkey::new_from_array([7u8; 32]),
            Pubkey::new_from_array([9u8; 32]),
            Pubkey::new_from_array([3u8; 32]),
        ]
    }

    /// Fixed contents, so fixture bytes are reproducible across runs.
    fn legacy_tx() -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::from([1u8; 64])],
            message: VersionedMessage::Legacy(Message {
                header: header(),
                account_keys: keys(),
                recent_blockhash: Hash::new_from_array([5u8; 32]),
                instructions: vec![instruction()],
            }),
        }
    }

    fn v0_tx() -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::from([2u8; 64])],
            message: VersionedMessage::V0(v0::Message {
                header: header(),
                account_keys: keys(),
                recent_blockhash: Hash::new_from_array([5u8; 32]),
                instructions: vec![instruction()],
                address_table_lookups: vec![],
            }),
        }
    }

    fn v1_tx() -> VersionedTransaction {
        let config = v1::TransactionConfig {
            priority_fee: Some(5_000),
            compute_unit_limit: Some(400_000),
            loaded_accounts_data_size_limit: Some(65_536),
            heap_size: None,
        };
        VersionedTransaction {
            signatures: vec![Signature::from([3u8; 64])],
            message: VersionedMessage::V1(v1::Message::new(
                header(),
                config,
                Hash::new_from_array([5u8; 32]),
                keys(),
                vec![instruction()],
            )),
        }
    }

    fn base_meta() -> TransactionStatusMeta {
        TransactionStatusMeta {
            status: Ok(()),
            fee: 5000,
            pre_balances: vec![100_000, 0],
            post_balances: vec![94_900, 100],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: Some(200),
            cost_units: Some(1234),
        }
    }

    fn token_balance(index: u8) -> TransactionTokenBalance {
        TransactionTokenBalance {
            account_index: index,
            mint: Pubkey::new_from_array([4u8; 32]).to_string(),
            ui_token_amount: UiTokenAmount {
                ui_amount: Some(100.0),
                decimals: 6,
                amount: "100000000".to_string(),
                ui_amount_string: "100".to_string(),
            },
            owner: Pubkey::new_from_array([6u8; 32]).to_string(),
            program_id: Pubkey::new_from_array([8u8; 32]).to_string(),
        }
    }

    /// Every field populated, so a round trip proves nothing is dropped.
    fn full_meta() -> TransactionStatusMeta {
        TransactionStatusMeta {
            status: Err(TransactionError::InstructionError(
                1,
                InstructionError::Custom(9),
            )),
            fee: 5000,
            pre_balances: vec![100_000, 0, 1],
            post_balances: vec![94_900, 100, 1],
            inner_instructions: Some(vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    instruction: CompiledInstruction {
                        program_id_index: 2,
                        accounts: vec![0, 1],
                        data: vec![1, 2, 3],
                    },
                    stack_height: Some(2),
                }],
            }]),
            log_messages: Some(vec!["Program log: hello".to_string()]),
            pre_token_balances: Some(vec![token_balance(1)]),
            post_token_balances: Some(vec![token_balance(1), token_balance(2)]),
            rewards: Some(vec![Reward {
                pubkey: Pubkey::new_from_array([10u8; 32]).to_string(),
                lamports: -5,
                post_balance: 95,
                reward_type: Some(solana_reward_info::RewardType::Voting),
                commission: Some(10),
                commission_bps: Some(1_000),
            }]),
            loaded_addresses: LoadedAddresses {
                writable: vec![Pubkey::new_from_array([11u8; 32])],
                readonly: vec![Pubkey::new_from_array([12u8; 32])],
            },
            return_data: Some(TransactionReturnData {
                program_id: Pubkey::new_from_array([13u8; 32]),
                data: vec![10, 20, 30],
            }),
            compute_units_consumed: Some(4_321),
            cost_units: Some(9_876),
        }
    }

    /// One representative of every metadata shape a row can hold.
    fn meta_cases() -> Vec<(&'static str, TransactionStatusMeta)> {
        let mut err_unit = base_meta();
        err_unit.status = Err(TransactionError::AccountNotFound);
        let mut err_struct = base_meta();
        err_struct.status = Err(TransactionError::InsufficientFundsForRent { account_index: 2 });
        let fees_only = TransactionStatusMeta {
            status: Err(TransactionError::AccountNotFound),
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            compute_units_consumed: None,
            cost_units: None,
            ..base_meta()
        };
        vec![
            ("success", base_meta()),
            ("err_unit", err_unit),
            ("err_struct", err_struct),
            ("fees_only", fees_only),
            ("every_field", full_meta()),
        ]
    }

    fn stored(transaction: VersionedTransaction, meta: TransactionStatusMeta) -> StoredTransaction {
        StoredTransaction {
            slot: 42,
            block_time: 1_700_000_000,
            transaction,
            meta: meta.into(),
        }
    }

    /// The rows pinned by committed fixtures.
    fn fixture_cases() -> Vec<(&'static str, StoredTransaction)> {
        vec![
            ("legacy_success", stored(legacy_tx(), base_meta())),
            ("legacy_failed", stored(legacy_tx(), full_meta())),
            ("v1_with_config", stored(v1_tx(), base_meta())),
        ]
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn from_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("fixture is valid hex"))
            .collect()
    }

    fn read_fixture(name: &str) -> Vec<u8> {
        let path = format!("{FIXTURE_DIR}/{name}.hex");
        let hex = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("missing fixture {path}; see the ignored generator"));
        from_hex(hex.trim())
    }

    /// Decoding must never panic, whatever bytes it is handed.
    fn decode_without_panicking(bytes: &[u8]) -> Result<StoredTransaction, StoredRowError> {
        let owned = bytes.to_vec();
        std::panic::catch_unwind(move || StoredTransaction::from_bytes(&owned))
            .expect("decoding a stored row must never panic")
    }

    /// Nothing a row holds may be lost on the way to disk and back, including
    /// the compute and cost units the previous layout dropped.
    #[test]
    fn every_meta_shape_round_trips_losslessly() {
        for (name, meta) in meta_cases() {
            let row = stored(legacy_tx(), meta.clone());
            let bytes = row
                .to_bytes()
                .unwrap_or_else(|e| panic!("{name}: encode: {e}"));
            let back = StoredTransaction::from_bytes(&bytes)
                .unwrap_or_else(|e| panic!("{name}: decode: {e}"));
            assert_eq!(back, row, "{name}: row changed on the way through");
            let restored: TransactionStatusMeta = back.meta.into();
            assert_eq!(
                restored, meta,
                "{name}: metadata changed on the way through"
            );
        }
    }

    /// The transaction is stored as the exact bytes its sender signed, for
    /// every version, and reads back unchanged.
    #[test]
    fn transactions_of_every_version_are_stored_as_wire_bytes() {
        for (name, tx) in [("legacy", legacy_tx()), ("v0", v0_tx()), ("v1", v1_tx())] {
            let row = stored(tx.clone(), base_meta());
            let bytes = row
                .to_bytes()
                .unwrap_or_else(|e| panic!("{name}: encode: {e}"));

            let wire = wincode::serialize(&tx).expect("wire encode");
            let len_bytes: [u8; 8] = bytes[TX_LEN_OFFSET..TX_LEN_OFFSET + 8]
                .try_into()
                .expect("length prefix");
            let start = TX_LEN_OFFSET + 8;
            assert_eq!(u64::from_le_bytes(len_bytes) as usize, wire.len(), "{name}");
            assert_eq!(&bytes[start..start + wire.len()], wire.as_slice(), "{name}");

            let back = StoredTransaction::from_bytes(&bytes)
                .unwrap_or_else(|e| panic!("{name}: decode: {e}"));
            assert_eq!(back.transaction, tx, "{name}: transaction changed");
        }
    }

    /// Committed bytes pin the format in both directions, so any change to it,
    /// ours or upstream's, fails here instead of in a database.
    #[test]
    fn rows_match_the_committed_fixtures() {
        for (name, row) in fixture_cases() {
            let expected = read_fixture(name);
            let actual = row
                .to_bytes()
                .unwrap_or_else(|e| panic!("{name}: encode: {e}"));
            assert_eq!(to_hex(&actual), to_hex(&expected), "{name}: encoding moved");
            let decoded = StoredTransaction::from_bytes(&expected)
                .unwrap_or_else(|e| panic!("{name}: decode: {e}"));
            assert_eq!(decoded, row, "{name}: decoding moved");
        }
    }

    /// Rewrites the committed fixtures. Run deliberately, never in CI, and only
    /// alongside a format version bump:
    /// `cargo test -p private-channel-core -- --ignored regenerate_stored_row_fixtures`
    #[test]
    #[ignore]
    fn regenerate_stored_row_fixtures() {
        let dir = std::path::Path::new(FIXTURE_DIR);
        std::fs::create_dir_all(dir).expect("create fixture dir");
        for (name, row) in fixture_cases() {
            let bytes = row.to_bytes().expect("encode");
            std::fs::write(dir.join(format!("{name}.hex")), to_hex(&bytes)).expect("write");
        }
    }

    #[test]
    fn every_row_starts_with_the_format_header() {
        let bytes = stored(legacy_tx(), base_meta()).to_bytes().expect("encode");
        assert_eq!(&bytes[..ROW_MAGIC.len()], &ROW_MAGIC);
        assert_eq!(bytes[ROW_MAGIC.len()], ROW_FORMAT_VERSION);
    }

    /// Damaged or foreign rows are refused with an error that says why.
    #[test]
    fn malformed_rows_are_refused_without_panicking() {
        let valid = stored(legacy_tx(), base_meta()).to_bytes().expect("encode");
        let mut future_version = valid.clone();
        future_version[ROW_MAGIC.len()] = ROW_FORMAT_VERSION + 1;
        let mut wrong_magic = valid.clone();
        wrong_magic[0] ^= 0xFF;
        let mut trailing = valid.clone();
        trailing.push(0);

        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty", vec![]),
            ("header only", valid[..ROW_HEADER_LEN].to_vec()),
            ("wrong magic", wrong_magic),
            ("future version", future_version),
            ("truncated", valid[..valid.len() - 10].to_vec()),
            ("trailing byte", trailing),
            ("garbage", vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11]),
        ];
        for (name, bytes) in cases {
            let err =
                decode_without_panicking(&bytes).expect_err(&format!("{name}: must be refused"));
            assert!(!err.to_string().is_empty(), "{name}: error must say why");
        }

        let mut version_bumped = valid;
        version_bumped[ROW_MAGIC.len()] = 9;
        assert!(matches!(
            decode_without_panicking(&version_bumped),
            Err(StoredRowError::UnsupportedVersion(9))
        ));
    }

    /// A row whose transaction bytes do not decode is refused, not half read.
    #[test]
    fn a_row_with_undecodable_transaction_bytes_is_refused() {
        let meta: StoredTransactionMeta = base_meta().into();
        let mut bytes = ROW_MAGIC.to_vec();
        bytes.push(ROW_FORMAT_VERSION);
        row_codec()
            .serialize_into(
                &mut bytes,
                &StoredRowRef {
                    slot: 1,
                    block_time: 2,
                    transaction: &[0xFF, 0xFF, 0xFF],
                    meta: &meta,
                },
            )
            .expect("encode");
        assert!(matches!(
            decode_without_panicking(&bytes),
            Err(StoredRowError::MalformedTransaction(_))
        ));
    }

    /// A row written before this format existed is refused rather than misread.
    #[test]
    fn a_row_from_the_previous_format_is_refused() {
        let previous = read_fixture("previous_format_success");
        assert!(matches!(
            decode_without_panicking(&previous),
            Err(StoredRowError::UnknownFormat)
        ));
    }

    #[test]
    fn transaction_with_status_meta_is_complete() {
        let row = stored(legacy_tx(), base_meta());
        match row.transaction_with_status_meta() {
            TransactionWithStatusMeta::Complete(versioned) => {
                assert_eq!(versioned.meta, base_meta());
            }
            _ => panic!("expected the complete variant"),
        }
    }

    /// A stored v1 transaction encodes for callers that accept v1, and is
    /// refused with the unsupported version error for callers that do not.
    #[test]
    fn encoding_respects_the_callers_version_ceiling() {
        let legacy = stored(legacy_tx(), base_meta());
        let encoded = legacy
            .encoded_transaction(&UiTransactionEncoding::Json, Some(0))
            .expect("legacy encodes");
        assert_eq!(encoded.slot, 42);
        assert_eq!(encoded.block_time, Some(1_700_000_000));

        let v1 = stored(v1_tx(), base_meta());
        let encoded = v1
            .encoded_transaction(&UiTransactionEncoding::Json, Some(1))
            .expect("v1 encodes when the caller accepts it");
        assert_eq!(
            encoded.transaction.version,
            Some(TransactionVersion::Number(1))
        );

        assert!(matches!(
            v1.encoded_transaction(&UiTransactionEncoding::Json, None),
            Err(EncodeError::UnsupportedTransactionVersion(1))
        ));
    }
}
