import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import httpx

from validator.common.types import QueryLog
from validator.api.s3_repository import S3Repository
from validator.config import S3Config

if TYPE_CHECKING:
    from validator.auth import TokenProvider

logger = logging.getLogger(__name__)


class LogsClient:
    def __init__(
        self,
        s3_config: S3Config,
        registry_url: str = "",
        token_provider: "TokenProvider | None" = None,
    ):
        self.s3 = S3Repository(s3_config)
        self.registry_url = registry_url.rstrip("/") if registry_url else ""
        self._token_provider = token_provider
        self._client = httpx.AsyncClient(timeout=60)

    async def close(self):
        await self._client.aclose()

    async def get_miner_configs(self, epoch_id: str) -> Optional[dict[str, dict]]:
        """Load miner-configs.json from S3.

        Returns raw dicts to support both flat format
        {"target_usd_per_cu": 0.001} / {"target_usd_per_million_cu": 1000}
        and enriched format {"default": {...}, "archive": {...}}.
        """
        key = self.s3.key("epochs", epoch_id, "miner-configs.json")
        data = await self.s3.get_object_json(key)
        if data is not None:
            logger.info(
                f"Loaded configs for {len(data)} miners from epochs/{epoch_id}/miner-configs.json"
            )
        return data

    # --- Registry API ---

    async def _get_registry_headers(self) -> dict[str, str]:
        """Build auth headers for registry requests."""
        headers: dict[str, str] = {}
        if self._token_provider:
            await self._token_provider.ensure_authenticated()
            if self._token_provider.access_token:
                headers["Authorization"] = f"Bearer {self._token_provider.access_token}"
        return headers

    async def _registry_get(self, path: str) -> httpx.Response:
        """GET a registry endpoint with JWT auth and 401 retry."""
        headers = await self._get_registry_headers()
        resp = await self._client.get(f"{self.registry_url}{path}", headers=headers)
        if resp.status_code == 401 and self._token_provider:
            refreshed = await self._token_provider.refresh()
            if not refreshed:
                refreshed = await self._token_provider.reauthenticate()
            if refreshed:
                headers = await self._get_registry_headers()
                resp = await self._client.get(
                    f"{self.registry_url}{path}", headers=headers
                )
        return resp

    async def fetch_finalized_epochs(self, limit: int = 20) -> Optional[list[dict]]:
        if not self.registry_url:
            return None
        try:
            resp = await self._registry_get(f"/epochs?limit={limit}")
            resp.raise_for_status()
            epochs = resp.json().get("epochs", [])
            logger.info(f"Fetched {len(epochs)} finalized epochs from registry")
            return epochs
        except Exception as e:
            logger.warning(f"Failed to fetch finalized epochs: {e}")
            return None

    # --- Manifest-based S3 access ---

    @staticmethod
    def _aggregate_cu_allocations(data: dict, totals: dict[str, dict]) -> None:
        """Aggregate CU allocations from a single file into totals.

        Handles both schema_version=1 (no archive dimension) and
        schema_version=2 (archive field on request_type_cus).
        """
        schema_version = data.get("schema_version", 1)
        for alloc in data.get("allocations", []):
            hk = alloc["miner_hotkey"]
            if hk not in totals:
                totals[hk] = {"total": 0, "archive": 0, "non_archive": 0}

            if schema_version >= 2:
                for entry in alloc.get("request_type_cus", []):
                    cu = entry.get("cu", 0)
                    totals[hk]["total"] += cu
                    if entry.get("archive", False):
                        totals[hk]["archive"] += cu
                    else:
                        totals[hk]["non_archive"] += cu
            else:
                total = alloc.get("total_cus", 0)
                totals[hk]["total"] += total
                totals[hk]["non_archive"] += total

    async def fetch_cu_allocations_from_manifest(
        self, manifest: dict
    ) -> dict[str, dict]:
        totals: dict[str, dict] = {}

        for gw_id, files in manifest.items():
            key = files.get("cu_allocation")
            if not key:
                continue
            try:
                data = await self.s3.get_object_json(key)
                if data is None:
                    logger.warning(f"Manifest CU file missing for {gw_id}: {key}")
                    continue
                self._aggregate_cu_allocations(data, totals)
                logger.info(
                    f"CU from manifest ({gw_id}): {len(data.get('allocations', []))} miners"
                )
            except Exception as e:
                logger.error(f"Error fetching manifest CU for {gw_id}: {e}")

        total_cu = sum(v["total"] for v in totals.values())
        logger.info(f"CU from manifest: {len(totals)} miners, {total_cu:,} total CU")
        return totals

    async def fetch_epoch_logs_from_manifest(
        self, manifest: dict, epoch_id: str
    ) -> list[QueryLog]:
        """Load all JSONL log files referenced in a finalized epoch manifest."""
        all_logs: list[QueryLog] = []
        file_count = 0

        for gw_id, files in manifest.items():
            for file_key in files.get("log_files", []):
                if not file_key.endswith(".jsonl"):
                    continue
                logs = await self._download_jsonl(file_key, epoch_id)
                all_logs.extend(logs)
                file_count += 1

        logger.info(
            f"Loaded {len(all_logs)} logs from {file_count} files for epoch {epoch_id}"
        )
        return all_logs

    # --- S3 helpers ---

    async def _download_jsonl(self, file_key: str, epoch_id: str) -> list[QueryLog]:
        try:
            content = await self.s3.get_object_text(file_key)
            if content is None:
                return []
            logs = []
            for line in content.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    log = self._parse_log_entry(entry, epoch_id)
                    if log:
                        logs.append(log)
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue
            return logs
        except Exception as e:
            logger.error(f"Error downloading {file_key}: {e}")
            return []

    def _parse_log_entry(self, entry: dict, epoch_id: str) -> Optional[QueryLog]:
        try:
            raw_id = entry.get("id")
            if isinstance(raw_id, int):
                log_id = raw_id
            elif isinstance(raw_id, str):
                try:
                    log_id = int(raw_id)
                except ValueError:
                    log_id = hash(raw_id) % (2**31)
            else:
                log_id = hash(str(raw_id)) % (2**31)

            ts = entry.get("timestamp", "")
            if ts:
                timestamp = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            else:
                timestamp = datetime.now(timezone.utc)

            params = entry.get("params")
            if isinstance(params, dict) and "params" not in params:
                params = {"params": params}
            elif params is None:
                params = {"params": []}

            target_usd_per_cu = entry.get("target_usd_per_cu")
            if (
                target_usd_per_cu is None
                and entry.get("target_usd_per_million_cu") is not None
            ):
                target_usd_per_cu = entry.get("target_usd_per_million_cu") / 1_000_000
            return QueryLog(
                id=log_id,
                timestamp=timestamp,
                epoch_id=epoch_id,
                chain=entry.get("chain", "ETH"),
                method=entry.get("method", "unknown"),
                block_number=entry.get("block_number"),
                params=params,
                miner_hotkey=entry.get("miner_hotkey") or "unknown",
                miner_coldkey=entry.get("miner_coldkey") or "unknown",
                node_id=entry.get("node_id", "unknown"),
                status_code=entry.get("status_code", 200),
                response_hash=entry.get("response_hash"),
                cu_cost=entry.get("cu_cost", 1),
                latency_ms=entry.get("latency_ms", 0),
                target_usd_per_cu=target_usd_per_cu,
                inferred_from_latest=entry.get("inferred_from_latest"),
            )
        except Exception:
            return None
