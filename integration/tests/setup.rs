#![allow(unused)]

use {
    solana_sdk::{
        account::AccountSharedData,
        hash::Hash,
        message::{v0, Message, VersionedMessage},
        program_pack::Pack,
        pubkey::Pubkey,
        signature::Keypair,
        signer::Signer,
        transaction::{Transaction, VersionedTransaction},
    },
    solana_system_interface::program as system_program,
    spl_associated_token_account::{
        get_associated_token_address, get_associated_token_address_with_program_id,
    },
    spl_token::state::{Account as TokenAccount, Mint},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    Legacy,
    V0,
}

pub fn get_token_account_balance(data: &[u8]) -> u64 {
    TokenAccount::unpack(data).unwrap().amount
}

pub fn create_mint_account_transaction(
    payer: &Keypair,
    mint_keypair: &Keypair,
    mint_authority: &Pubkey,
    decimals: u8,
    recent_blockhash: Hash,
) -> Transaction {
    // PrivateChannel's admin VM creates the account automatically, so only initialize_mint
    // is needed and the mint keypair does NOT sign.
    let init_mint_ix = spl_token::instruction::initialize_mint(
        &spl_token::id(),
        &mint_keypair.pubkey(),
        mint_authority,
        None,
        decimals,
    )
    .unwrap();

    Transaction::new_signed_with_payer(
        &[init_mint_ix],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    )
}

pub fn mint_to_transaction(
    payer: &Keypair,
    mint: &Pubkey,
    destination: &Pubkey,
    mint_authority: &Pubkey,
    amount: u64,
    recent_blockhash: Hash,
) -> Transaction {
    let mint_to_ix = spl_token::instruction::mint_to(
        &spl_token::id(),
        mint,
        destination,
        mint_authority,
        &[],
        amount,
    )
    .unwrap();

    Transaction::new_signed_with_payer(
        &[mint_to_ix],
        Some(&payer.pubkey()),
        &[payer],
        recent_blockhash,
    )
}

pub fn transfer_tokens_transaction(
    from: &Keypair,
    to: &Pubkey,
    mint: &Pubkey,
    amount: u64,
    recent_blockhash: Hash,
) -> Transaction {
    let from_token_account = get_associated_token_address(&from.pubkey(), mint);
    let to_token_account = get_associated_token_address(to, mint);
    let transfer_ix = spl_token::instruction::transfer(
        &spl_token::id(),
        &from_token_account,
        &to_token_account,
        &from.pubkey(),
        &[],
        amount,
    )
    .unwrap();

    Transaction::new_signed_with_payer(
        &[transfer_ix],
        Some(&from.pubkey()),
        &[from],
        recent_blockhash,
    )
}

pub fn transfer_tokens_versioned_transaction(
    from: &Keypair,
    to: &Pubkey,
    mint: &Pubkey,
    amount: u64,
    recent_blockhash: Hash,
    tx_type: TransactionType,
) -> VersionedTransaction {
    let from_token_account = get_associated_token_address(&from.pubkey(), mint);
    let to_token_account = get_associated_token_address(to, mint);
    let transfer_ix = spl_token::instruction::transfer(
        &spl_token::id(),
        &from_token_account,
        &to_token_account,
        &from.pubkey(),
        &[],
        amount,
    )
    .unwrap();

    let message = match tx_type {
        TransactionType::Legacy => {
            let legacy_message = Message::new_with_blockhash(
                &[transfer_ix],
                Some(&from.pubkey()),
                &recent_blockhash,
            );
            VersionedMessage::Legacy(legacy_message)
        }
        TransactionType::V0 => {
            let v0_message =
                v0::Message::try_compile(&from.pubkey(), &[transfer_ix], &[], recent_blockhash)
                    .unwrap();
            VersionedMessage::V0(v0_message)
        }
    };

    VersionedTransaction::try_new(message, &[from]).unwrap()
}

pub fn withdraw_funds_transaction(
    from: &Keypair,
    mint: &Pubkey,
    amount: u64,
    recent_blockhash: Hash,
) -> Transaction {
    use private_channel_withdraw_program_client::instructions::WithdrawFundsBuilder;

    let token_account =
        get_associated_token_address_with_program_id(&from.pubkey(), mint, &spl_token::ID);

    let withdraw_ix = WithdrawFundsBuilder::new()
        .user(from.pubkey())
        .mint(*mint)
        .token_account(token_account)
        .token_program(spl_token::id())
        .associated_token_program(spl_associated_token_account::id())
        .amount(amount)
        .instruction();

    Transaction::new_signed_with_payer(
        &[withdraw_ix],
        Some(&from.pubkey()),
        &[from],
        recent_blockhash,
    )
}

pub fn empty_transaction(payer: &Keypair, recent_blockhash: Hash) -> Transaction {
    Transaction::new_signed_with_payer(&[], Some(&payer.pubkey()), &[payer], recent_blockhash)
}

pub fn mixed_transaction(
    admin: &Keypair,
    non_admin: &Keypair,
    mint: &Pubkey,
    destination: &Pubkey,
    mint_authority: &Pubkey,
    amount: u64,
    recent_blockhash: Hash,
) -> Transaction {
    let init_mint_ix =
        spl_token::instruction::initialize_mint(&spl_token::id(), mint, mint_authority, None, 3)
            .unwrap();

    let from_token_account = get_associated_token_address(&non_admin.pubkey(), mint);
    let to_token_account = get_associated_token_address(&admin.pubkey(), mint);
    let transfer_ix = spl_token::instruction::transfer(
        &spl_token::id(),
        &from_token_account,
        &to_token_account,
        &non_admin.pubkey(),
        &[],
        amount,
    )
    .unwrap();

    Transaction::new_signed_with_payer(
        &[init_mint_ix, transfer_ix],
        Some(&admin.pubkey()),
        &[admin, non_admin],
        recent_blockhash,
    )
}

pub fn mint_account() -> AccountSharedData {
    mint_account_with_authority(&Pubkey::default())
}

pub fn mint_account_with_authority(mint_authority: &Pubkey) -> AccountSharedData {
    let mut data = [0; Mint::LEN];
    Mint::pack(
        Mint {
            supply: 100_000_000,
            decimals: 0,
            is_initialized: true,
            mint_authority: Some(*mint_authority).into(),
            ..Default::default()
        },
        &mut data,
    )
    .unwrap();

    let rent_exempt_balance = 1_461_600;
    let mut account = AccountSharedData::new(rent_exempt_balance, data.len(), &spl_token::id());
    account.set_data_from_slice(&data);
    account
}

pub fn system_account(lamports: u64) -> AccountSharedData {
    AccountSharedData::new(lamports, 0, &system_program::id())
}

pub fn token_account(owner: &Pubkey, mint: &Pubkey, amount: u64) -> AccountSharedData {
    let mut data = [0; TokenAccount::LEN];
    TokenAccount::pack(
        TokenAccount {
            mint: *mint,
            owner: *owner,
            amount,
            state: spl_token::state::AccountState::Initialized,
            ..Default::default()
        },
        &mut data,
    )
    .unwrap();

    let rent_exempt_balance = 2_039_280;
    let mut account = AccountSharedData::new(rent_exempt_balance, data.len(), &spl_token::id());
    account.set_data_from_slice(&data);
    account
}
