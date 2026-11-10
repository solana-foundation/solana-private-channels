"""Sends one Token-2022 MintTo (by the channel admin) or Burn (by the owner) on the channel.

usage: chan_tx.py <write-rpc> <read-rpc> mint|burn <signer keypair json> <mint> <owner pubkey> <amount>
Prints the signature. Used to over-issue and then retire supply for the halt test.
"""
import base64
import json
import sys
import urllib.request

from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.transaction import Transaction

TOKEN_2022 = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
ATA_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")


def rpc(url, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=20))


write, read, op, kp_path, mint_s, owner_s, amount = sys.argv[1:8]
signer = Keypair.from_bytes(bytes(json.load(open(kp_path))))
mint, owner = Pubkey.from_string(mint_s), Pubkey.from_string(owner_s)
ata, _ = Pubkey.find_program_address([bytes(owner), bytes(TOKEN_2022), bytes(mint)], ATA_PROGRAM)
tag = 7 if op == "mint" else 8
# MintTo: mint, destination, authority. Burn: account, mint, owner.
metas = ([AccountMeta(mint, False, True), AccountMeta(ata, False, True), AccountMeta(signer.pubkey(), True, False)]
         if op == "mint" else
         [AccountMeta(ata, False, True), AccountMeta(mint, False, True), AccountMeta(signer.pubkey(), True, False)])
ix = Instruction(TOKEN_2022, bytes([tag]) + int(amount).to_bytes(8, "little"), metas)
bh = Hash.from_string(rpc(read, "getLatestBlockhash", [])["result"]["value"]["blockhash"])
tx = Transaction([signer], Message([ix], signer.pubkey()), bh)
res = rpc(write, "sendTransaction", [base64.b64encode(bytes(tx)).decode(), {"encoding": "base64"}])
if "error" in res:
    sys.exit("send failed: " + json.dumps(res["error"])[:300])
print(res["result"])
