"""Simulates a Token-2022 MintTo signed by a given admin keypair on a channel node.

Usage: old-admin-mintto.py <read-node-rpc> <admin-keypair.json> <mint> <owner-pubkey>
Prints the simulation error (or "ok") and exits 0 only when the node refused it.
The read node serves simulateTransaction with the same admin rules as the write node.
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
    req = urllib.request.Request(url, json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
                                 {"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=20))


url, kp_path, mint_s, owner_s = sys.argv[1:5]
admin = Keypair.from_bytes(bytes(json.load(open(kp_path))))
mint, owner = Pubkey.from_string(mint_s), Pubkey.from_string(owner_s)
ata, _ = Pubkey.find_program_address([bytes(owner), bytes(TOKEN_2022), bytes(mint)], ATA_PROGRAM)
# MintTo: tag 7, u64 amount; accounts mint, destination, authority.
ix = Instruction(TOKEN_2022, bytes([7]) + (1).to_bytes(8, "little"),
                 [AccountMeta(mint, False, True), AccountMeta(ata, False, True), AccountMeta(admin.pubkey(), True, False)])
bh = Hash.from_string(rpc(url, "getLatestBlockhash", [])["result"]["value"]["blockhash"])
tx = Transaction([admin], Message([ix], admin.pubkey()), bh)
res = rpc(url, "simulateTransaction", [base64.b64encode(bytes(tx)).decode(), {"encoding": "base64", "sigVerify": True}])
err = res.get("error") or (res.get("result") or {}).get("value", {}).get("err")
print(json.dumps(err) if err else "ok")
sys.exit(0 if err else 1)
