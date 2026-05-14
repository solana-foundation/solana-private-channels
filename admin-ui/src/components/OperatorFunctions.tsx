import { useState } from 'react';
import { useSolana } from '../hooks/useSolana';
import { useWallet } from '../hooks/useWallet';
import { useWalletStandardAccount } from '../hooks/useWalletStandardAccount';
import { useCluster } from '../hooks/useCluster';
import { address } from '@solana/addresses';
import { useWalletAccountTransactionSendingSigner } from '@solana/react';
import { getBase58Decoder } from '@solana/codecs-strings';
import { getReleaseFundsInstructionAsync, getResetSmtRootInstructionAsync } from '@private-channel-escrow';
import { findAssociatedTokenPda } from '@solana-program/token';
import {
  pipe,
  createTransactionMessage,
  setTransactionMessageFeePayerSigner,
  setTransactionMessageLifetimeUsingBlockhash,
  appendTransactionMessageInstruction,
  signAndSendTransactionMessageWithSigners,
  assertIsTransactionMessageWithSingleSendingSigner,
} from '@solana/kit';

const TOKEN_PROGRAM_ADDRESS = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' as const;

interface OperatorFunctionsProps {
  instancePubkey: string;
}

export function OperatorFunctions({ instancePubkey }: OperatorFunctionsProps) {
  const { connected } = useWallet();
  const account = useWalletStandardAccount();
  const { network } = useCluster();

  if (!connected || !account) {
    return (
      <div className="card">
        <h2>Operator Functions</h2>
        <p className="card-description">These functions require operator privileges</p>
        <div className="error-message">Please connect your wallet to use operator functions</div>
      </div>
    );
  }

  return <OperatorFunctionsContent instancePubkey={instancePubkey} account={account} network={network} />;
}

interface OperatorFunctionsContentProps {
  instancePubkey: string;
  account: Parameters<typeof useWalletAccountTransactionSendingSigner>[0];
  network: string;
}

function OperatorFunctionsContent({ instancePubkey, account, network }: OperatorFunctionsContentProps) {
  const { rpc } = useSolana();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState<string | null>(null);
  const [mintAddress, setMintAddress] = useState('');
  const [userAddress, setUserAddress] = useState('');
  const [amount, setAmount] = useState('');
  const [newWithdrawalRoot, setNewWithdrawalRoot] = useState('');
  const [transactionNonce, setTransactionNonce] = useState('');
  const [siblingProofs, setSiblingProofs] = useState('');

  const chainId = (network === 'localnet' ? 'solana:devnet' : `solana:${network}`) as `solana:${string}`;
  const transactionSigner = useWalletAccountTransactionSendingSigner(account, chainId);

  const handleReleaseFunds = async () => {
    if (!mintAddress || !userAddress || !amount || !newWithdrawalRoot || !transactionNonce || !siblingProofs) {
      setError('Please fill in all fields');
      return;
    }

    try {
      setLoading(true);
      setError('');
      setSuccess(null);

      // Convert hex strings to Uint8Array
      const withdrawalRootHex = newWithdrawalRoot.startsWith('0x') ? newWithdrawalRoot.slice(2) : newWithdrawalRoot;
      const withdrawalRootBytes = new Uint8Array(withdrawalRootHex.match(/.{1,2}/g)!.map(byte => parseInt(byte, 16)));

      const proofsHex = siblingProofs.startsWith('0x') ? siblingProofs.slice(2) : siblingProofs;
      const proofsBytes = new Uint8Array(proofsHex.match(/.{1,2}/g)!.map(byte => parseInt(byte, 16)));

      if (withdrawalRootBytes.length !== 32) {
        throw new Error('newWithdrawalRoot must be 32 bytes');
      }
      if (proofsBytes.length !== 512) {
        throw new Error('siblingProofs must be 512 bytes');
      }

      // Find user ATA
      const [userAta] = await findAssociatedTokenPda({
        mint: address(mintAddress),
        owner: address(userAddress),
        tokenProgram: address(TOKEN_PROGRAM_ADDRESS),
      });

      // Get the release funds instruction
      const instruction = await getReleaseFundsInstructionAsync({
        payer: transactionSigner,
        operator: transactionSigner,
        instance: address(instancePubkey),
        mint: address(mintAddress),
        userAta,
        amount: BigInt(amount),
        user: address(userAddress),
        newWithdrawalRoot: withdrawalRootBytes,
        transactionNonce: BigInt(transactionNonce),
        siblingProofs: proofsBytes,
      });

      console.log('Created release funds instruction:', instruction);

      // Get recent blockhash
      const { value: latestBlockhash } = await rpc.getLatestBlockhash({ commitment: 'confirmed' }).send();

      // Build transaction message
      const transactionMessage = pipe(
        createTransactionMessage({ version: 0 }),
        (m) => setTransactionMessageFeePayerSigner(transactionSigner, m),
        (m) => setTransactionMessageLifetimeUsingBlockhash(latestBlockhash, m),
        (m) => appendTransactionMessageInstruction(instruction, m)
      );

      console.log('Transaction message:', transactionMessage);

      // Assert single sending signer
      assertIsTransactionMessageWithSingleSendingSigner(transactionMessage);

      // Sign and send the transaction
      const signatureBytes = await signAndSendTransactionMessageWithSigners(transactionMessage);

      // Convert signature bytes to base58 string
      const signature = getBase58Decoder().decode(signatureBytes);

      console.log('Transaction sent with signature:', signature);

      setSuccess(`Funds released successfully! Signature: ${signature}`);
      setMintAddress('');
      setUserAddress('');
      setAmount('');
      setNewWithdrawalRoot('');
      setTransactionNonce('');
      setSiblingProofs('');

    } catch (err) {
      console.error('Error releasing funds:', err);
      setError(err instanceof Error ? err.message : 'Failed to release funds');
    } finally {
      setLoading(false);
    }
  };

  const handleResetSmtRoot = async () => {
    try {
      setLoading(true);
      setError('');
      setSuccess(null);

      // Get the reset SMT root instruction
      const instruction = await getResetSmtRootInstructionAsync({
        payer: transactionSigner,
        operator: transactionSigner,
        instance: address(instancePubkey),
      });

      console.log('Created reset SMT root instruction:', instruction);

      // Get recent blockhash
      const { value: latestBlockhash } = await rpc.getLatestBlockhash({ commitment: 'confirmed' }).send();

      // Build transaction message
      const transactionMessage = pipe(
        createTransactionMessage({ version: 0 }),
        (m) => setTransactionMessageFeePayerSigner(transactionSigner, m),
        (m) => setTransactionMessageLifetimeUsingBlockhash(latestBlockhash, m),
        (m) => appendTransactionMessageInstruction(instruction, m)
      );

      console.log('Transaction message:', transactionMessage);

      // Assert single sending signer
      assertIsTransactionMessageWithSingleSendingSigner(transactionMessage);

      // Sign and send the transaction
      const signatureBytes = await signAndSendTransactionMessageWithSigners(transactionMessage);

      // Convert signature bytes to base58 string
      const signature = getBase58Decoder().decode(signatureBytes);

      console.log('Transaction sent with signature:', signature);

      setSuccess(`SMT root reset successfully! Signature: ${signature}`);

    } catch (err) {
      console.error('Error resetting SMT root:', err);
      setError(err instanceof Error ? err.message : 'Failed to reset SMT root');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card">
      <h2>Operator Functions</h2>
      <p className="card-description">These functions require operator privileges</p>

      {error && <div className="error-message">{error}</div>}

      {success && (
        <div style={{ marginTop: '1rem', padding: '1rem', backgroundColor: 'rgba(76, 175, 80, 0.2)', borderRadius: '8px' }}>
          <p style={{ margin: 0, color: '#4caf50', fontWeight: 'bold', marginBottom: '0.5rem' }}>
            {success.split('!')[0]}!
          </p>
          <p style={{ margin: 0, fontSize: '0.85rem', wordBreak: 'break-all' }}>
            Signature: {success.split('Signature: ')[1]}
          </p>
        </div>
      )}

      <div className="function-section">
        <h3>Release Funds</h3>
        <div className="form-group">
          <label>Mint Address</label>
          <input
            type="text"
            value={mintAddress}
            onChange={(e) => setMintAddress(e.target.value)}
            placeholder="Enter token mint address"
            className="input"
          />
        </div>
        <div className="form-group">
          <label>User Address</label>
          <input
            type="text"
            value={userAddress}
            onChange={(e) => setUserAddress(e.target.value)}
            placeholder="Enter user wallet address"
            className="input"
          />
        </div>
        <div className="form-group">
          <label>Amount</label>
          <input
            type="number"
            value={amount}
            onChange={(e) => setAmount(e.target.value)}
            placeholder="Enter amount to release"
            className="input"
          />
        </div>
        <div className="form-group">
          <label>New Withdrawal Root (32 bytes hex)</label>
          <input
            type="text"
            value={newWithdrawalRoot}
            onChange={(e) => setNewWithdrawalRoot(e.target.value)}
            placeholder="0x..."
            className="input"
          />
        </div>
        <div className="form-group">
          <label>Transaction Nonce</label>
          <input
            type="number"
            value={transactionNonce}
            onChange={(e) => setTransactionNonce(e.target.value)}
            placeholder="Enter transaction nonce"
            className="input"
          />
        </div>
        <div className="form-group">
          <label>Sibling Proofs (512 bytes hex)</label>
          <textarea
            value={siblingProofs}
            onChange={(e) => setSiblingProofs(e.target.value)}
            placeholder="0x..."
            className="input textarea"
            rows={3}
          />
        </div>
        <button
          onClick={handleReleaseFunds}
          disabled={loading || !mintAddress || !userAddress || !amount || !newWithdrawalRoot || !transactionNonce || !siblingProofs}
          className="button button-primary"
        >
          {loading ? 'Processing...' : 'Release Funds'}
        </button>
      </div>

      <div className="function-section">
        <h3>Reset SMT Root</h3>
        <p className="info-text">
          This will reset the Sparse Merkle Tree root to the empty tree state
        </p>
        <button
          onClick={handleResetSmtRoot}
          disabled={loading}
          className="button button-warning"
        >
          {loading ? 'Processing...' : 'Reset SMT Root'}
        </button>
      </div>
    </div>
  );
}
