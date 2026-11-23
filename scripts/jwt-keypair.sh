#!/usr/bin/env bash
set -euo pipefail

# jwt-keypair.sh: print a fresh Ed25519 keypair for login tokens.
#   JWT_SIGNING_KEY  base58 64-byte keypair, for auth only
#   JWT_PUBLIC_KEY   base58 public key, for auth and the gateway
# Needs openssl with Ed25519 (1.1.1 or later). The private key only travels through pipes.

openssl genpkey -algorithm ed25519 -outform DER | python3 -c '
import subprocess, sys

# PKCS#8 header of an Ed25519 key (RFC 8410), followed by the 32-byte seed.
PREFIX = bytes.fromhex("302e020100300506032b657004220420")
der = sys.stdin.buffer.read()
if len(der) != 48 or not der.startswith(PREFIX):
    sys.exit("ERROR: openssl returned no Ed25519 key (needs OpenSSL 1.1.1 or later)")
public = subprocess.run(
    ["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"],
    input=der, capture_output=True, check=True,
).stdout[-32:]

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def b58(data):
    n, out = int.from_bytes(data, "big"), ""
    while n:
        n, digit = divmod(n, 58)
        out = ALPHABET[digit] + out
    return "1" * (len(data) - len(data.lstrip(b"\0"))) + out

print("JWT_SIGNING_KEY=" + b58(der[16:] + public))
print("JWT_PUBLIC_KEY=" + b58(public))
'
