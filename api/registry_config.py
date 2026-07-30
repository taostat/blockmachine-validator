"""Client for fetching network-wide validator config from the registry.

The registry serves authoritative values for consensus-critical settings
(epoch, scoring, reference nodes, S3 log location, CU schedule, …) so all
validators stay in sync without per-deployment env vars.
"""

import logging
from pathlib import Path

import httpx

from validator.auth import TokenProvider

logger = logging.getLogger(__name__)

_VERSION_FILE = Path(__file__).parent.parent / "VERSION"


def get_validator_version() -> str:
    """Read the validator version from the VERSION file."""
    try:
        return _VERSION_FILE.read_text().strip()
    except FileNotFoundError:
        return "unknown"


class RegistryConfigClient:
    def __init__(self, registry_url: str, token_provider: TokenProvider):
        self._url = registry_url.rstrip("/")
        self._token = token_provider
        self._http = httpx.AsyncClient(timeout=30)
        self.version = get_validator_version()

    async def close(self) -> None:
        await self._http.aclose()

    def _headers(self) -> dict:
        headers = {"X-Validator-Version": self.version}
        if self._token.access_token:
            headers["Authorization"] = f"Bearer {self._token.access_token}"
        return headers

    async def fetch(self) -> dict:
        await self._token.ensure_authenticated()
        resp = await self._http.get(
            f"{self._url}/validator/config", headers=self._headers()
        )
        # 401 recovery, mirroring GatewayReferenceClient / RegistryBlacklist:
        # ensure_authenticated() never re-checks expiry once a token exists,
        # so an expired JWT would otherwise 401 every config refresh forever
        # (observed live: a validator's refreshes failed for 27h until
        # restart). Refresh — or fall back to a full challenge re-auth — and
        # retry once.
        if resp.status_code == 401:
            logger.info("Registry config token expired, attempting refresh...")
            refreshed = await self._token.refresh()
            if not refreshed:
                refreshed = await self._token.reauthenticate()
            if refreshed:
                resp = await self._http.get(
                    f"{self._url}/validator/config", headers=self._headers()
                )
            else:
                self._token.set_backoff(60.0)
        resp.raise_for_status()
        return resp.json()
