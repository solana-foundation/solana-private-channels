import {
    Address,
    BaseTransactionSignerConfig,
    SignaturesMap,
    TransactionMessageBytes,
    TransactionSigner,
    TransactionWithLifetime,
} from '@solana/kit';

/**
 * Creates a mock TransactionSigner for testing purposes
 */
export const mockTransactionSigner = (address: Address): TransactionSigner => ({
    address,
    async signTransactions(
        transactions: readonly (Readonly<{
            messageBytes: TransactionMessageBytes;
            signatures: SignaturesMap;
        }> &
            TransactionWithLifetime)[],
        _config?: BaseTransactionSignerConfig,
    ): Promise<
        readonly (Readonly<{
            messageBytes: TransactionMessageBytes;
            signatures: SignaturesMap;
        }> &
            TransactionWithLifetime)[]
    > {
        return transactions;
    },
});

/**
 * Common test addresses for consistent testing
 */
export const TEST_ADDRESSES = {
    USDC_MINT: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' as Address,
    WRAPPED_SOL: 'So11111111111111111111111111111111111111112' as Address,
    PAYER: '11111111111111111111111111111112' as Address,
    ADMIN: '4aMgkHVGzK3FAhWvJRpCpG2kTkA4dxUQSGfPbhpZsDbF' as Address,
    INSTANCE_SEED: 'inStv3wR1in1k8keGBLUDPJPgtbcqKYA7LRUyFC2NdG' as Address,
    INSTANCE_SEED_2: 'jNsTv4xR2jn2k9kfHCMVEQKQhtcdrLZB8MRVzGD3NeH' as Address,
    INSTANCE: '5JYdXKJLwfCWQdR7aBe6L1zjwvBJHXEemMVgXQM97C8V' as Address,
    OPERATOR: '6cPFGPZbUE7DQPrw24GgTYNkvr2FLnHfgqgjCxEn73K5' as Address,
    WALLET: '7BgH7Hq2P3CsQQ2DgJtfHPNNdJtKJsKJGJhRPNkkvuY3' as Address,
    MINT: 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' as Address,
    ALLOWED_MINT: '8MKrYq1F8xKhXp4FJWfYSgYZNgPqvP3DQa2Jv7rXfQN8' as Address,
    INSTANCE_ATA: '9LqZxwCF5N4FdpTJGcZpYPvT2GcLXMdNzQf5EyN5DhYx' as Address,
    EVENT_AUTHORITY: '8WtLfmC99djequwMiicZij6p6t1hsyhUDqSSwJoAvJEK' as Address,
} as const;

/**
 * Test nonce for withdrawal transactions
 */
export const TEST_TRANSACTION_NONCE = 1;

/**
 * Expected program address for all instructions
 */
export const EXPECTED_PROGRAM_ADDRESS = '8msahYFvfeiiz3C2NzAorhfhAes5GaEThzmLWSLUHNkK';
