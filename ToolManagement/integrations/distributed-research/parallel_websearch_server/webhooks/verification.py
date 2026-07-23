"""Webhook signature verification using HMAC."""

import hashlib
import hmac
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class WebhookVerificationError(Exception):
    """Raised when webhook verification fails."""

    pass


class WebhookVerifier:
    """
    Verifies webhook signatures using HMAC-SHA256.

    Security features:
    - HMAC-SHA256 signature verification
    - Timestamp validation to prevent replay attacks
    - Constant-time comparison to prevent timing attacks
    """

    def __init__(
        self,
        secret: str,
        tolerance_seconds: int = 300  # 5 minutes
    ):
        """
        Initialize verifier.

        Args:
            secret: Webhook secret for HMAC computation
            tolerance_seconds: Max age of webhook events to accept
        """
        self.secret = secret.encode() if isinstance(secret, str) else secret
        self.tolerance_seconds = tolerance_seconds

    def verify_signature(
        self,
        payload: bytes,
        signature_header: str,
        timestamp_header: Optional[str] = None
    ) -> bool:
        """
        Verify webhook signature.

        Args:
            payload: Raw request body (bytes)
            signature_header: Value of X-Parallel-Signature header
            timestamp_header: Value of X-Parallel-Timestamp header (optional)

        Returns:
            True if signature is valid

        Raises:
            WebhookVerificationError: If verification fails
        """
        # Validate timestamp if provided (prevent replay attacks)
        if timestamp_header:
            try:
                event_timestamp = int(timestamp_header)
                current_timestamp = int(time.time())

                age = current_timestamp - event_timestamp

                if age > self.tolerance_seconds:
                    raise WebhookVerificationError(
                        f"Webhook event too old: {age}s (tolerance: {self.tolerance_seconds}s)"
                    )

                if age < -self.tolerance_seconds:
                    raise WebhookVerificationError(
                        "Webhook event timestamp is in the future"
                    )

            except ValueError as e:
                raise WebhookVerificationError(f"Invalid timestamp header: {e}")

        # Extract signature from header
        # Header format: "t=<timestamp>,v1=<signature>" or just "<signature>"
        signature = self._extract_signature(signature_header)

        # Compute expected signature
        if timestamp_header:
            # Include timestamp in signature payload (Stripe-style)
            signed_payload = f"{timestamp_header}.{payload.decode()}"
            expected_signature = hmac.new(
                self.secret,
                signed_payload.encode(),
                hashlib.sha256
            ).hexdigest()
        else:
            # Simple HMAC of payload
            expected_signature = hmac.new(
                self.secret,
                payload,
                hashlib.sha256
            ).hexdigest()

        # Constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(signature, expected_signature):
            logger.warning("Webhook signature verification failed")
            raise WebhookVerificationError("Invalid signature")

        logger.debug("Webhook signature verified successfully")
        return True

    def _extract_signature(self, signature_header: str) -> str:
        """Extract signature from header value."""
        # Support multiple formats:
        # 1. Stripe-style: "t=1234567890,v1=abc123..."
        # 2. GitHub-style: "sha256=abc123..."
        # 3. Simple: "abc123..."

        if "," in signature_header:
            # Stripe-style
            parts = dict(part.split("=", 1) for part in signature_header.split(","))
            return parts.get("v1", "")

        elif "=" in signature_header:
            # GitHub-style
            algo, sig = signature_header.split("=", 1)
            if algo != "sha256":
                raise WebhookVerificationError(f"Unsupported signature algorithm: {algo}")
            return sig

        else:
            # Simple hex signature
            return signature_header

    def generate_signature(
        self,
        payload: bytes,
        timestamp: Optional[int] = None
    ) -> str:
        """
        Generate HMAC signature for a payload.

        Useful for testing webhook handlers.

        Args:
            payload: Payload to sign
            timestamp: Optional timestamp to include

        Returns:
            Signature hex string
        """
        if timestamp:
            signed_payload = f"{timestamp}.{payload.decode()}"
            return hmac.new(
                self.secret,
                signed_payload.encode(),
                hashlib.sha256
            ).hexdigest()
        else:
            return hmac.new(
                self.secret,
                payload,
                hashlib.sha256
            ).hexdigest()

    def generate_headers(
        self,
        payload: bytes
    ) -> dict:
        """
        Generate webhook headers for testing.

        Args:
            payload: Payload to sign

        Returns:
            Dict of headers including signature and timestamp
        """
        timestamp = int(time.time())
        signature = self.generate_signature(payload, timestamp)

        return {
            "X-Parallel-Signature": f"t={timestamp},v1={signature}",
            "X-Parallel-Timestamp": str(timestamp),
            "Content-Type": "application/json"
        }


# Convenience function for FastAPI dependency injection
def verify_webhook(
    secret: str,
    payload: bytes,
    signature: str,
    timestamp: Optional[str] = None
) -> bool:
    """
    Verify webhook signature (convenience function).

    Args:
        secret: Webhook secret
        payload: Raw request body
        signature: Signature header value
        timestamp: Timestamp header value

    Returns:
        True if valid

    Raises:
        WebhookVerificationError: If verification fails
    """
    verifier = WebhookVerifier(secret)
    return verifier.verify_signature(payload, signature, timestamp)
