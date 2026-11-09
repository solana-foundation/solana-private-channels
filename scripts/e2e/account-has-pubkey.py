"""Exit 0 when the account's data holds the given pubkey: account-has-pubkey.py <rpc> <account> <pubkey>."""
import base64
import json
import sys
import urllib.request

A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
rpc, account, pubkey = sys.argv[1:4]
req = {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo", "params": [account, {"encoding": "base64"}]}
r = json.load(urllib.request.urlopen(
    urllib.request.Request(rpc, json.dumps(req).encode(), {"Content-Type": "application/json"}), timeout=15))
value = r.get("result", {}).get("value")
if not value:
    sys.exit(1)
data = base64.b64decode(value["data"][0])
n = 0
for c in pubkey:
    n = n * 58 + A.index(c)
sys.exit(0 if n.to_bytes(32, "big") in data else 1)
