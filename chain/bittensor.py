import asyncio
import logging
import time
from typing import Any

import bittensor as bt

from validator.common.types import MinerInfo

logger = logging.getLogger(__name__)


class _KeypairWallet:
    """Shim so bt.Subtensor.sign_and_send_extrinsic sees .hotkey."""

    def __init__(self, hotkey: bt.Keypair):
        self.hotkey = hotkey

    def unlock_hotkey(self) -> None:
        # Hotkey is already loaded in memory; no-op to satisfy newer bittensor versions
        pass


class BittensorChain:
    """
    Real bittensor chain interface.
    Wraps subtensor + metagraph behind ChainInterface protocol.
    """

    def __init__(
        self,
        network: str,
        netuid: int,
        hotkey: bt.Keypair,
        metagraph_cache_ttl: int = 120,
    ):
        self.netuid = netuid
        self.hotkey = hotkey
        self._network = network
        self.subtensor = bt.Subtensor(network=network)
        self._subtensor_lock = asyncio.Lock()
        self._metagraph_cache_ttl = metagraph_cache_ttl
        self._metagraph_cached_at: float = 0
        self._cached_miners: list[MinerInfo] | None = None
        self._consecutive_failures: int = 0

        logger.info(
            f"BittensorChain initialized: network={network}, "
            f"netuid={netuid}, hotkey={self.hotkey.ss58_address}"
        )

    def _reconnect(self) -> None:
        """Recreate the subtensor connection after persistent failures."""
        logger.warning("Reconnecting subtensor (stale websocket)")
        try:
            self.subtensor = bt.Subtensor(network=self._network)
            self._consecutive_failures = 0
            logger.info("Subtensor reconnected successfully")
        except Exception as e:
            logger.error(f"Subtensor reconnect failed: {e}")

    async def _maybe_reconnect(self) -> None:
        """Reconnect if we've hit 3+ consecutive chain failures."""
        if self._consecutive_failures >= 3:
            await asyncio.to_thread(self._reconnect)

    async def get_current_block(self) -> int:
        async with self._subtensor_lock:
            await self._maybe_reconnect()
            try:
                result = await asyncio.to_thread(self.subtensor.get_current_block)
                self._consecutive_failures = 0
                return result
            except Exception:
                self._consecutive_failures += 1
                raise

    async def get_tempo(self) -> int:
        async with self._subtensor_lock:
            await self._maybe_reconnect()
            try:
                result = await asyncio.to_thread(self.subtensor.tempo, self.netuid)
                self._consecutive_failures = 0
                return result
            except Exception:
                self._consecutive_failures += 1
                raise

    async def set_weights(self, uids: list[int], weights: list[int]) -> bool:
        async with self._subtensor_lock:
            await self._maybe_reconnect()
            try:
                result = await asyncio.to_thread(
                    self.subtensor.set_weights,
                    wallet=_KeypairWallet(self.hotkey),
                    netuid=self.netuid,
                    uids=uids,
                    weights=weights,
                    wait_for_inclusion=True,
                    wait_for_finalization=True,
                )
                self._consecutive_failures = 0
            except Exception:
                self._consecutive_failures += 1
                raise
        return self._parse_chain_result(result, "set_weights")

    async def get_miners(self) -> list[MinerInfo]:
        async with self._subtensor_lock:
            now = time.monotonic()
            if (
                self._cached_miners is not None
                and (now - self._metagraph_cached_at) < self._metagraph_cache_ttl
            ):
                return self._cached_miners

            await self._maybe_reconnect()
            logger.info(f"Syncing metagraph for netuid {self.netuid}")
            try:
                metagraph = await asyncio.to_thread(
                    self.subtensor.metagraph, self.netuid
                )
                self._consecutive_failures = 0
            except Exception:
                self._consecutive_failures += 1
                raise

            miners = []
            validator_hotkey = self.hotkey.ss58_address

            for uid in range(metagraph.n):
                hotkey = metagraph.hotkeys[uid]
                coldkey = metagraph.coldkeys[uid]

                if not hotkey or hotkey == validator_hotkey:
                    continue

                miners.append(MinerInfo(uid=uid, hotkey=hotkey, coldkey=coldkey))

            logger.info(f"Metagraph synced: {len(miners)} miners")
            self._cached_miners = miners
            self._metagraph_cached_at = now
            return miners

    def get_validator_hotkey(self) -> str:
        return self.hotkey.ss58_address

    def sign(self, data: bytes) -> bytes:
        return self.hotkey.sign(data)

    async def get_pending_weight_commits(self) -> list[dict] | None:
        """
        Query SubtensorModule.WeightCommits for this validator's pending commits.
        Returns a list of dicts with commit info, or None on error.
        """
        try:
            hotkey = self.hotkey.ss58_address
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                result = self.subtensor.substrate.query(
                    "SubtensorModule", "WeightCommits", [self.netuid, hotkey]
                )
                if asyncio.iscoroutine(result):
                    result = await result
            if not result or not result.value:
                return []
            commits = []
            for entry in result.value:
                hash_val, commit_block, first_reveal_block, last_reveal_block = entry
                commits.append(
                    {
                        "hash": str(hash_val),
                        "commit_block": int(commit_block),
                        "first_reveal_block": int(first_reveal_block),
                        "last_reveal_block": int(last_reveal_block),
                    }
                )
            return commits
        except Exception as e:
            self._consecutive_failures += 1
            logger.debug(f"Could not query pending weight commits: {e}")
            return None

    async def get_blocks_since_last_update(self) -> int | None:
        try:
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                metagraph = await asyncio.to_thread(
                    self.subtensor.metagraph, self.netuid
                )
                current = await asyncio.to_thread(self.subtensor.get_current_block)
                self._consecutive_failures = 0
            validator_hotkey = self.hotkey.ss58_address
            for uid in range(metagraph.n):
                if metagraph.hotkeys[uid] == validator_hotkey:
                    return current - int(metagraph.last_update[uid])
            return None
        except Exception as e:
            self._consecutive_failures += 1
            logger.warning(f"Could not fetch blocks since last update: {e}")
            return None

    async def get_validator_last_update_block(self) -> int | None:
        """return the block number of this validator's last weight update, or None if not found"""
        try:
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                metagraph = await asyncio.to_thread(
                    self.subtensor.metagraph, self.netuid
                )
                self._consecutive_failures = 0
            validator_hotkey = self.hotkey.ss58_address
            for uid in range(metagraph.n):
                if metagraph.hotkeys[uid] == validator_hotkey:
                    return int(metagraph.last_update[uid])
            return None
        except Exception as e:
            self._consecutive_failures += 1
            logger.debug(f"Could not fetch validator last update block: {e}")
            return None

    def _parse_chain_result(self, result, label: str) -> bool:
        if isinstance(result, tuple):
            success = result[0]
            message = result[1] if len(result) > 1 else ""
            if not success:
                logger.error(f"{label} rejected: {message}")
            return bool(success)
        return bool(result)

    def get_subtensor(self) -> Any:
        return self.subtensor
