"""One-off: mint a fresh MCP operator token using the deployed TOKEN_SECRET.

The signing secret never leaves Modal — only the derived token is returned.
Usage: modal run mint_operator_token.py
"""

import json
import time

import modal

app = modal.App("mint-mcp-operator-token")


@app.function(
    secrets=[modal.Secret.from_name("tool-registry-oauth-core")],
    timeout=60,
)
def mint() -> dict:
    import base64
    import hashlib
    import hmac
    import os

    token_secret = os.environ["TOKEN_SECRET"]
    claims = {"c": "operator", "t": time.time(), "s": "mcp tools"}
    payload = json.dumps(claims).encode()
    sig = hmac.new(token_secret.encode(), payload, hashlib.sha256).hexdigest()[:16]
    token = base64.urlsafe_b64encode(payload).decode() + "." + sig

    # sanity: verify it decodes with the same secret
    payload_b64, sig_ret = token.rsplit(".", 1)
    expected = hmac.new(token_secret.encode(), base64.urlsafe_b64decode(payload_b64), hashlib.sha256).hexdigest()[:16]
    assert hmac.compare_digest(sig_ret, expected), "self-verification failed"

    # also report whether DEFAULT_BEARER_TOKEN is set as an alternate auth path
    print("MCP_OPERATOR_TOKEN=" + token, flush=True)
    return {
        "token": token,
        "claims": claims,
        "has_default_bearer": bool(os.environ.get("DEFAULT_BEARER_TOKEN")),
    }
