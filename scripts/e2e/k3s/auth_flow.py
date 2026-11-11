"""Public-path RBAC probe against the node's 8899/8903. Prints status codes only.

usage: auth_flow.py <host> <user keypair json> <own channel ATA> <other channel ATA>
"""
import json
import sys
import urllib.error
import urllib.request
import uuid

from solders.keypair import Keypair

host, kp_path, own_ata, other_ata = sys.argv[1:5]
AUTH, GW = f"http://{host}:8903", f"http://{host}:8899"


def post(url, body, token=None):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"content-type": "application/json"})
    if token:
        req.add_header("authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, None


def rpc(method, params, token=None):
    return post(GW, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, token)[0]


name, pw = "e2e" + uuid.uuid4().hex[:8], uuid.uuid4().hex
results = {"register": post(f"{AUTH}/auth/register", {"username": name, "password": pw})[0]}
code, body = post(f"{AUTH}/auth/login", {"username": name, "password": pw})
results["login"] = code
token = body["token"]
kp = Keypair.from_json(open(kp_path).read())
results["no_token_getBlock"] = rpc("getBlock", [1])
results["no_token_getAccountInfo"] = rpc("getAccountInfo", [str(kp.pubkey())])
results["user_getBlock"] = rpc("getBlock", [1], token)
results["unlinked_balance"] = rpc("getBalance", [str(kp.pubkey())], token)
c, ch = post(f"{AUTH}/auth/challenge-wallet", {"pubkey": str(kp.pubkey())}, token)
sig = kp.sign_message(ch["message"].encode())
results["link"] = post(f"{AUTH}/auth/verify-wallet", {"pubkey": str(kp.pubkey()), "nonce": ch["nonce"], "signature": str(sig)}, token)[0]
results["linked_own_balance"] = rpc("getTokenAccountBalance", [own_ata], token)
results["linked_other_balance"] = rpc("getTokenAccountBalance", [other_ata], token)
results["bad_token"] = rpc("getSignaturesForAddress", [str(kp.pubkey())], token[:-4] + "AAAA")
json.dump({"user": name, "results": results}, sys.stdout)
