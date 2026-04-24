import logging
import random
import time
from typing import Any, Protocol

import httpx

from validator.common.types import Chain
from validator.auth import TokenProvider
from validator.config import ReferenceNodesConfig

logger = logging.getLogger(__name__)


class ReferenceClient(Protocol):
    async def get_block_number(self) -> int: ...
    async def query(self, method: str, params: list) -> Any: ...
    async def get_block_hash(self, block_number: int) -> str: ...


class EthereumReferenceClient:
    def __init__(self, endpoint: str, timeout_ms: int = 10000):
        self.endpoint = endpoint
        self.timeout = timeout_ms / 1000
        self._client = httpx.AsyncClient(timeout=self.timeout)
        self._request_id = random.randint(1, 2**31)

    async def close(self):
        await self._client.aclose()

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def query(self, method: str, params: list) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_id(),
        }
        response = await self._client.post(
            self.endpoint, json=payload, headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise Exception(f"RPC error: {data['error']}")
        return data.get("result")

    async def get_block_number(self) -> int:
        result = await self.query("eth_blockNumber", [])
        return int(result, 16)

    async def get_block_hash(self, block_number: int) -> str:
        block = await self.query("eth_getBlockByNumber", [hex(block_number), False])
        if block:
            return block.get("hash", "")
        return ""


class SubstrateReferenceClient:
    def __init__(self, endpoint: str, timeout_ms: int = 10000):
        self.endpoint = endpoint
        self.timeout = timeout_ms / 1000
        self._client = httpx.AsyncClient(timeout=self.timeout)
        self._request_id = random.randint(1, 2**31)
        self._is_ws = endpoint.startswith("ws")

    async def close(self):
        await self._client.aclose()

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def query(self, method: str, params: list) -> Any:
        http_endpoint = self.endpoint
        if self._is_ws:
            http_endpoint = self.endpoint.replace("wss://", "https://").replace(
                "ws://", "http://"
            )
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_id(),
        }
        response = await self._client.post(
            http_endpoint, json=payload, headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise Exception(f"RPC error: {data['error']}")
        return data.get("result")

    async def get_block_number(self) -> int:
        header = await self.query("chain_getHeader", [])
        if header:
            return int(header.get("number", "0x0"), 16)
        return 0

    async def get_block_hash(self, block_number: int) -> str:
        result = await self.query("chain_getBlockHash", [block_number])
        return result if result else ""


class GatewayReferenceClient:
    def __init__(
        self,
        gateway_url: str,
        chain: str,
        token_provider: TokenProvider,
        timeout_ms: int = 10000,
    ):
        self.gateway_url = gateway_url.rstrip("/")
        self.chain = chain.lower()
        self._token_provider = token_provider
        self.timeout = timeout_ms / 1000
        self._client = httpx.AsyncClient(timeout=self.timeout)
        self._request_id = random.randint(1, 2**31)
        self._consecutive_auth_failures = 0
        self._auth_give_up_threshold = 10
        self._auth_gave_up_at = 0.0
        self._auth_recovery_secs = 300.0  # retry after 5 min cooldown

    async def close(self):
        await self._client.aclose()

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _get_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token_provider.access_token:
            headers["Authorization"] = f"Bearer {self._token_provider.access_token}"
        return headers

    async def query(self, method: str, params: list) -> Any:
        # If we previously gave up, check if the cooldown has elapsed
        if (
            self._consecutive_auth_failures > self._auth_give_up_threshold
            and self._auth_gave_up_at > 0
        ):
            if time.monotonic() - self._auth_gave_up_at < self._auth_recovery_secs:
                raise Exception(
                    f"Gateway auth cooling down (chain={self.chain}) — "
                    f"will retry after {self._auth_recovery_secs}s"
                )
            # Cooldown elapsed — reset and allow a fresh attempt
            logger.info(
                "Gateway auth cooldown elapsed (chain=%s), retrying", self.chain
            )
            self._consecutive_auth_failures = 0
            self._auth_gave_up_at = 0.0

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_id(),
        }
        url = f"{self.gateway_url}/rpc/{self.chain}"
        response = await self._client.post(
            url, json=payload, headers=self._get_headers()
        )

        if response.status_code == 401:
            self._consecutive_auth_failures += 1
            if self._consecutive_auth_failures > self._auth_give_up_threshold:
                self._auth_gave_up_at = time.monotonic()
                raise Exception(
                    f"Gateway auth permanently failing (chain={self.chain}) — "
                    f"giving up after {self._auth_give_up_threshold} retries, "
                    f"will retry in {self._auth_recovery_secs}s"
                )
            noisy = self._consecutive_auth_failures <= 3
            if noisy:
                logger.info("Gateway token expired, attempting refresh...")
            elif self._consecutive_auth_failures == 4:
                logger.warning(
                    "Gateway auth failing repeatedly (chain=%s) — "
                    "suppressing logs, will give up at %d retries",
                    self.chain,
                    self._auth_give_up_threshold,
                )
            refreshed = await self._token_provider.refresh()
            if not refreshed:
                refreshed = await self._token_provider.reauthenticate()
            if refreshed:
                response = await self._client.post(
                    url, json=payload, headers=self._get_headers()
                )
                if response.status_code != 401:
                    self._consecutive_auth_failures = 0
                else:
                    self._token_provider.set_backoff(60.0)
        else:
            self._consecutive_auth_failures = 0

        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise Exception(f"RPC error: {data['error']}")
        return data.get("result")

    async def get_block_number(self) -> int:
        if self.chain == "tao":
            header = await self.query("chain_getHeader", [])
            if header:
                return int(header.get("number", "0x0"), 16)
            return 0
        else:
            result = await self.query("eth_blockNumber", [])
            return int(result, 16)

    async def get_block_hash(self, block_number: int) -> str:
        if self.chain == "tao":
            result = await self.query("chain_getBlockHash", [block_number])
            return result if result else ""
        else:
            block = await self.query("eth_getBlockByNumber", [hex(block_number), False])
            if block:
                return block.get("hash", "")
            return ""


CHAIN_ALIASES = {
    "SYSTEM": Chain.TAO.value,
    "CHAIN": Chain.TAO.value,
    "STATE": Chain.TAO.value,
    "AUTHOR": Chain.TAO.value,
    "SUBTENSORMODULE": Chain.TAO.value,
}


def normalize_chain_name(chain: str) -> str:
    return CHAIN_ALIASES.get(chain.upper(), chain.upper())


class ReferenceNodeManager:
    def __init__(
        self,
        config: ReferenceNodesConfig,
        timeout_ms: int = 10000,
        gateway_url: str = "",
        token_provider: TokenProvider | None = None,
    ):
        self.config = config
        self.timeout_ms = timeout_ms
        self._gateway_url = gateway_url
        self._token_provider = token_provider
        self._clients: dict[str, ReferenceClient] = {}

    async def initialize(self):
        if self._gateway_url and self._token_provider:
            await self._token_provider.ensure_authenticated()
            url = self._gateway_url
            tao = GatewayReferenceClient(
                url, "tao", self._token_provider, self.timeout_ms
            )
            eth = GatewayReferenceClient(
                url, "eth", self._token_provider, self.timeout_ms
            )
            bsc = GatewayReferenceClient(
                url, "bsc", self._token_provider, self.timeout_ms
            )
            logger.info(f"Initialized gateway reference clients via {url}")
        else:
            tao = SubstrateReferenceClient(self.config.tao, self.timeout_ms)
            eth = EthereumReferenceClient(self.config.eth, self.timeout_ms)
            bsc = EthereumReferenceClient(self.config.bsc, self.timeout_ms)
            logger.info("Initialized direct reference clients")

        self._clients[Chain.TAO.value] = tao
        self._clients[Chain.ETH.value] = eth
        self._clients[Chain.BSC.value] = bsc

        for alias, canonical in CHAIN_ALIASES.items():
            self._clients[alias] = self._clients[canonical]

    async def close(self):
        closed = set()
        for name, client in self._clients.items():
            if id(client) not in closed and hasattr(client, "close"):
                await client.close()
                closed.add(id(client))
        if self._token_provider:
            await self._token_provider.close()

    def get_client(self, chain: str) -> ReferenceClient | None:
        return self._clients.get(normalize_chain_name(chain))

    async def get_block_number(self, chain: str) -> int:
        client = self._clients.get(normalize_chain_name(chain))
        if not client:
            raise ValueError(f"No reference client for chain {chain}")
        return await client.get_block_number()

    async def query(self, chain: str, method: str, params: list) -> Any:
        normalized = normalize_chain_name(chain)
        client = self._clients.get(normalized)
        if not client:
            raise ValueError(f"No reference client for chain {chain}")
        return await client.query(method, params)

    async def get_block_hash(self, chain: str, block_number: int) -> str:
        client = self._clients.get(normalize_chain_name(chain))
        if not client:
            raise ValueError(f"No reference client for chain {chain}")
        return await client.get_block_hash(block_number)
