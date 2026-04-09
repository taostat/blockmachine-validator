"""Client for fetching network-wide validator config from the registry.

The registry serves authoritative values for consensus-critical settings
(epoch, scoring, reference nodes, S3 log location, CU schedule, …) so all
validators stay in sync without per-deployment env vars.
"""

import logging

import httpx

from validator.auth import TokenProvider

logger = logging.getLogger(__name__)


class RegistryConfigClient:
    def __init__(self, registry_url: str, token_provider: TokenProvider):
        self._url = registry_url.rstrip("/")
        self._token = token_provider
        self._http = httpx.AsyncClient(timeout=30)

    async def close(self) -> None:
        await self._http.aclose()

    async def fetch(self) -> dict:
        await self._token.ensure_authenticated()
        headers = {}
        if self._token.access_token:
            headers["Authorization"] = f"Bearer {self._token.access_token}"
        resp = await self._http.get(f"{self._url}/validator/config", headers=headers)
        resp.raise_for_status()
        return resp.json()
