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
        # bittensor >= 10 returns ExtrinsicResponse: tuple-LIKE (indexable,
        # len() == 2) but NOT a tuple, and it defines __len__ without
        # __bool__ — so bool(result) is ALWAYS True, success or not, and
        # its `success` field even defaults to True. Read the verdict
        # field; never the object's truthiness. (2026-08-20: a commit
        # rejected inside the rate-limit window logged "submitted
        # successfully" and the epoch was marked done with no retry.)
        success = getattr(result, "success", None)
        if success is not None:
            if not success:
                message = getattr(result, "message", None)
                error = getattr(result, "error", None)
                logger.error(f"{label} rejected: message={message!r} error={error!r}")
            return bool(success)
        if isinstance(result, tuple):
            success = result[0]
            message = result[1] if len(result) > 1 else ""
            if not success:
                logger.error(f"{label} rejected: {message}")
            return bool(success)
        if isinstance(result, bool):
            return result
        # An unrecognized response shape must never read as success —
        # that is the exact defect this function had.
        logger.error(
            f"{label}: unrecognized chain result type "
            f"{type(result).__name__}; treating as FAILURE"
        )
        return False

    async def verify_weight_commit_landed(self, since_block: int) -> dict | None:
        """Read TimelockedWeightCommits back from chain: did OUR commit land?

        The SDK's return value is the submitter reporting on itself;
        storage is the only proof. Three outcomes: landed / not landed /
        None (the chain could not be read — absence of an answer is not
        an answer, and the caller must not treat it as one).

        Checks the current epoch AND current+1: a commit submitted on the
        block where the epoch slot fires is tagged to the NEXT epoch by
        the chain's lookahead, so checking only the current index yields
        a false "didn't land" once per epoch. ``since_block`` scopes the
        check to THIS submit — without it any older commit of ours
        satisfies the read.

        Presence is necessary, not sufficient: reveal_round and
        ciphertext length are reported for cross-checking, but content
        correctness is the reveal watcher's job, not this read's.

        Storage key is [netuid_index, epoch] where netuid_index =
        mechanism_id * GLOBAL_MAX_SUBNET_COUNT + netuid; for the main
        mechanism (0) that is just the netuid.
        """
        # The whole body is fail-to-None: parsing a storage entry can
        # raise just as a read can, and an exception here must become
        # "could not verify", never bubble into the submit path (same
        # exception scope as get_pending_weight_commits above).
        try:
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                epoch_res = self.subtensor.substrate.query(
                    "SubtensorModule", "SubnetEpochIndex", [self.netuid]
                )
                if asyncio.iscoroutine(epoch_res):
                    epoch_res = await epoch_res
                epoch = (
                    int(epoch_res.value)
                    if epoch_res is not None and epoch_res.value is not None
                    else 0
                )
                # epoch-1 covers verification delayed across a rollover
                # (the commit landed under the previous index); epoch+1
                # covers the chain's lookahead tagging at a fire-block.
                # since_block keeps all three honest.
                epochs = [ep for ep in (epoch - 1, epoch, epoch + 1) if ep >= 0]
                raw: list = []
                for ep in epochs:
                    res = self.subtensor.substrate.query(
                        "SubtensorModule", "TimelockedWeightCommits", [self.netuid, ep]
                    )
                    if asyncio.iscoroutine(res):
                        res = await res
                    if res is not None and res.value:
                        raw.extend((ep, entry) for entry in res.value)

            ss58 = self.hotkey.ss58_address
            pub = getattr(self.hotkey, "public_key", None)
            pub_hex = (
                pub.hex().lower() if isinstance(pub, (bytes, bytearray)) else None
            )
            for ep, entry in raw:
                try:
                    who, commit_block, ciphertext, reveal_round = entry
                except (TypeError, ValueError):
                    logger.warning(
                        f"Unrecognized TimelockedWeightCommits entry shape: "
                        f"{type(entry).__name__}"
                    )
                    continue
                if not self._is_our_account(who, ss58, pub_hex):
                    continue
                if int(commit_block) >= since_block:
                    self._consecutive_failures = 0
                    return {
                        "landed": True,
                        "epoch": ep,
                        "commit_block": int(commit_block),
                        "reveal_round": int(reveal_round),
                        "ct_len": self._ciphertext_len(ciphertext),
                    }
            self._consecutive_failures = 0
            return {
                "landed": False,
                "epochs_checked": epochs,
                "since_block": since_block,
            }
        except Exception as e:
            self._consecutive_failures += 1
            logger.warning(f"Could not verify TimelockedWeightCommits: {e}")
            return None

    @staticmethod
    def _is_our_account(acct, ss58: str, pub_hex: str | None) -> bool:
        # substrate-interface decodes AccountId32 as SS58 or raw hex
        # depending on version/metadata; accept either representation.
        s = str(acct)
        if s == ss58:
            return True
        return pub_hex is not None and s.lower().removeprefix("0x") == pub_hex

    @staticmethod
    def _ciphertext_len(ciphertext) -> int | None:
        if isinstance(ciphertext, str):
            h = ciphertext.removeprefix("0x")
            return len(h) // 2
        try:
            return len(ciphertext)
        except TypeError:
            return None

    def get_subtensor(self) -> Any:
        return self.subtensor
