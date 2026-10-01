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
    WALLET: '7BgH7Hq2P3CsQQ2DgJtfHPNNdJtKJsKJGJhRPNkkvuY3' as Address,
    MINT: 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL' as Address,
    ADMIN: '4aMgkHVGzK3FAhWvJRpCpG2kTkA4dxUQSGfPbhpZsDbF' as Address,
    WITHDRAW_CONFIG: '8MKrYq1F8xKhXp4FJWfYSgYZNgPqvP3DQa2Jv7rXfQN8' as Address,
    TREASURY_TOKEN_ACCOUNT: '9LqZxwCF5N4FdpTJGcZpYPvT2GcLXMdNzQf5EyN5DhYx' as Address,
} as const;

/**
 * Expected program address for all instructions
 */
export const EXPECTED_PROGRAM_ADDRESS = 'J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi';
