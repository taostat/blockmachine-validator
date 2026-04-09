"""Registry-backed blacklist client — submits bans via REST API."""

import logging
import time

import httpx

from validator.auth import TokenProvider

logger = logging.getLogger(__name__)

# Auth recovery thresholds (mirrors GatewayReferenceClient)
_AUTH_GIVE_UP_THRESHOLD = 10
_AUTH_RECOVERY_SECS = 300.0  # 5 minutes


class RegistryBlacklistClient:
    """BlacklistService implementation backed by the registry /bans API.

    Maintains a local append-only cache so that:
    - ``is_blacklisted`` avoids repeated network calls for known bans
    - ``ban`` never POSTs the same coldkey twice
    """

    def __init__(self, registry_url: str, token_provider: TokenProvider):
        self._url = registry_url.rstrip("/")
        self._token = token_provider
        self._http = httpx.AsyncClient(timeout=30)
        self._banned_cache: set[str] = set()
        self._consecutive_auth_failures = 0
        self._auth_gave_up_at = 0.0

    async def close(self) -> None:
        await self._http.aclose()

    # --- BlacklistService protocol ---

    async def is_blacklisted(self, coldkey: str) -> bool:
        if coldkey in self._banned_cache:
            return True
        try:
            resp = await self._authed_request("GET", f"/bans/{coldkey}")
            banned = resp.json().get("banned", False)
            if banned:
                self._banned_cache.add(coldkey)
            return banned
        except Exception as e:
            logger.warning(f"Failed to check ban status for {coldkey[:16]}...: {e}")
            return coldkey in self._banned_cache

    async def ban(
        self,
        coldkey: str,
        hotkey: str,
        reason: str,
        epoch_id: str,
        evidence: dict,
    ) -> bool:
        # Dedup: don't POST if we already know this coldkey is banned
        if coldkey in self._banned_cache:
            logger.debug(f"Ban already submitted for {coldkey[:16]}..., skipping")
            return True
        try:
            resp = await self._authed_request(
                "POST",
                "/bans",
                json={
                    "coldkey": coldkey,
                    "hotkey": hotkey,
                    "reason": reason,
                    "epoch_id": epoch_id,
                    "evidence": evidence,
                },
            )
            self._banned_cache.add(coldkey)
            already = resp.json().get("already_banned", False)
            if not already:
                logger.info(
                    f"BANNED via registry API: coldkey={coldkey}, "
                    f"hotkey={hotkey[:20]}..., reason={reason}"
                )
            return True
        except Exception as e:
            logger.error(f"Failed to submit ban for {coldkey[:16]}...: {e}")
            return False

    async def get_banned_coldkeys(self) -> set[str]:
        """Fetch all banned coldkeys from the registry."""
        try:
            resp = await self._authed_request("GET", "/bans")
            bans = resp.json().get("bans", [])
            coldkeys = {b["coldkey"] for b in bans}
            self._banned_cache.update(coldkeys)
            return coldkeys
        except Exception as e:
            logger.warning(f"Failed to fetch ban list: {e}, using cache")
            return set(self._banned_cache)

    # --- Auth helpers (mirrors GatewayReferenceClient pattern) ---

    def _get_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._token.access_token:
            headers["Authorization"] = f"Bearer {self._token.access_token}"
        return headers

    async def _authed_request(self, method: str, path: str, **kwargs) -> httpx.Response:
        # Cooldown check
        if (
            self._consecutive_auth_failures > _AUTH_GIVE_UP_THRESHOLD
            and self._auth_gave_up_at > 0
        ):
            if time.monotonic() - self._auth_gave_up_at < _AUTH_RECOVERY_SECS:
                raise Exception(
                    "Registry auth cooling down — "
                    f"will retry after {_AUTH_RECOVERY_SECS}s"
                )
            logger.info("Registry auth cooldown elapsed, retrying")
            self._consecutive_auth_failures = 0
            self._auth_gave_up_at = 0.0

        await self._token.ensure_authenticated()
        url = f"{self._url}{path}"
        response = await self._http.request(
            method, url, headers=self._get_headers(), **kwargs
        )

        if response.status_code == 401:
            self._consecutive_auth_failures += 1
            if self._consecutive_auth_failures > _AUTH_GIVE_UP_THRESHOLD:
                self._auth_gave_up_at = time.monotonic()
                raise Exception(
                    f"Registry auth permanently failing — "
                    f"giving up after {_AUTH_GIVE_UP_THRESHOLD} retries, "
                    f"will retry in {_AUTH_RECOVERY_SECS}s"
                )
            noisy = self._consecutive_auth_failures <= 3
            if noisy:
                logger.info("Registry token expired, attempting refresh...")
            elif self._consecutive_auth_failures == 4:
                logger.warning(
                    "Registry auth failing repeatedly — suppressing logs, "
                    f"will give up at {_AUTH_GIVE_UP_THRESHOLD} retries"
                )
            refreshed = await self._token.refresh()
            if not refreshed:
                refreshed = await self._token.reauthenticate()
            if refreshed:
                response = await self._http.request(
                    method, url, headers=self._get_headers(), **kwargs
                )
                if response.status_code != 401:
                    self._consecutive_auth_failures = 0
                else:
                    self._token.set_backoff(60.0)
        else:
            self._consecutive_auth_failures = 0

        response.raise_for_status()
        return response
