"""JWT token provider for validator gateway authentication.

Authenticates via challenge-response: signs a nonce with the validator
hotkey, exchanges it for a JWT access token. No human interaction needed.
Tokens are held in memory and re-obtained via challenge flow on expiry.
"""

import asyncio
import logging
import time
from typing import Callable

import httpx

logger = logging.getLogger(__name__)


class TokenProvider:
    """Provides JWT access tokens for gateway authentication.

    Handles challenge flow auth, token refresh, and backoff.
    Thread-safe via async lock for concurrent 401 recovery.
    """

    def __init__(
        self,
        auth_url: str,
        client_id: str,
        netuid: int,
        hotkey_ss58: str,
        sign_fn: Callable[[bytes], bytes],
    ):
        self.access_token = ""
        self.refresh_token = ""
        self._auth_url = auth_url
        self._client_id = client_id
        self._netuid = netuid
        self._hotkey_ss58 = hotkey_ss58
        self._sign_fn = sign_fn
        self._backoff_until = 0.0
        self._lock = asyncio.Lock()
        self._http = httpx.AsyncClient(timeout=30)

    async def ensure_authenticated(self) -> None:
        """Ensure a token exists (from refresh or challenge flow).

        Does not validate token expiry — expired tokens are handled
        lazily via 401 recovery in GatewayReferenceClient.query().
        """
        if self.access_token:
            return
        if self.refresh_token:
            logger.info("Access token missing, attempting refresh...")
            if await self.refresh():
                return
            logger.warning("Refresh failed, falling back to challenge flow")
        logger.info("No validator tokens, starting challenge flow...")
        await self._challenge_flow()

    async def _challenge_flow(self) -> None:
        """Sign a nonce with the hotkey to obtain a JWT."""
        scopes = [f"subnet:{self._netuid}:validator"]

        resp = await self._http.post(
            f"{self._auth_url}/v1/auth/challenge",
            json={"address": self._hotkey_ss58, "scopes": scopes},
        )
        resp.raise_for_status()
        nonce = resp.json()["nonce"]

        signature = self._sign_fn(nonce.encode())
        signature_hex = "0x" + signature.hex()

        resp = await self._http.post(
            f"{self._auth_url}/v1/auth/verify",
            json={
                "nonce": nonce,
                "address": self._hotkey_ss58,
                "signature": signature_hex,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token", "")
        logger.info("Challenge flow authentication successful")

    async def reauthenticate(self) -> bool:
        """Re-run challenge flow (lock-protected with backoff)."""
        if time.monotonic() < self._backoff_until:
            return False
        token_before = self.access_token
        async with self._lock:
            if self.access_token != token_before:
                return True
            if time.monotonic() < self._backoff_until:
                return False
            try:
                await self._challenge_flow()
                self._backoff_until = 0.0
                return True
            except Exception as e:
                self._backoff_until = time.monotonic() + 60.0
                logger.warning("Challenge flow re-auth failed: %s — backing off 60s", e)
                return False

    async def refresh(self) -> bool:
        """Refresh the access token using the refresh token."""
        if not self.refresh_token or not self._auth_url:
            return False
        if time.monotonic() < self._backoff_until:
            return False
        token_before = self.access_token
        async with self._lock:
            if self.access_token != token_before:
                return True
            if time.monotonic() < self._backoff_until:
                return False
            try:
                response = await self._http.post(
                    f"{self._auth_url}/v1/oauth/refresh",
                    json={
                        "grant_type": "refresh_token",
                        "client_id": self._client_id,
                        "refresh_token": self.refresh_token,
                    },
                )
                response.raise_for_status()
                data = response.json()
                self.access_token = data["access_token"]
                if "refresh_token" in data:
                    self.refresh_token = data["refresh_token"]
                self._backoff_until = 0.0
                logger.info("Gateway token refreshed")
                return True
            except Exception as e:
                self._backoff_until = time.monotonic() + 60.0
                logger.warning("Gateway token refresh failed: %s — backing off 60s", e)
                return False

    def set_backoff(self, seconds: float = 60.0) -> None:
        self._backoff_until = time.monotonic() + seconds

    async def close(self) -> None:
        await self._http.aclose()
