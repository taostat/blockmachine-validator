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

    async def commit_reveal_enabled(self) -> bool | None:
        """``CommitRevealWeightsEnabled`` for this subnet, read from the chain.

        This decides which of two very different things ``set_weights``
        does. On: the SDK encrypts the vector to a drand round and the chain
        applies it when the round is published, so the vector reaches
        ``Weights`` at the next epoch boundary at best. Off: the SDK sends a
        plain ``set_weights`` and the vector is in ``Weights`` the block it
        is included. The submitter has to prove the right one, so it reads
        the flag rather than assuming it.

        ``None`` when the chain could not be read. The caller treats that as
        ON: the commit path is the one every deployed validator has run for
        months, and a wrong "off" would skip the commit window and prove the
        submission against storage the commit never writes.
        """
        async with self._subtensor_lock:
            await self._maybe_reconnect()
            try:
                result = await asyncio.to_thread(
                    self.subtensor.commit_reveal_enabled, self.netuid
                )
                self._consecutive_failures = 0
            except Exception as e:
                self._consecutive_failures += 1
                logger.warning(f"Could not read CommitRevealWeightsEnabled: {e}")
                return None
        return bool(result)

    async def set_weights(
        self,
        uids: list[int],
        weights: list[int],
        block_time: float | None = None,
    ) -> bool:
        """Submit weights. With commit-reveal on this is a timelocked commit
        and ``block_time`` steers the drand round we target; with it off the
        SDK sends a plain ``set_weights`` and ``block_time`` is not used (the
        SDK reads the flag itself, inside ``set_weights``).

        The SDK derives the reveal round by predicting the next boundary as
        ``blocks_remaining * block_time`` and then adding a 3-block safety
        offset. Its default of 12.0 therefore aims PAST the boundary and the
        reveal misses the epoch it was computed for. The caller computes a
        value that aims just before the earliest the boundary can arrive; see
        ``WeightLoop._reveal_block_time``.

        Passing it is conditional on the installed SDK accepting it: the
        parameter arrived in bittensor 10.x, and binding an unknown keyword
        would raise before any extrinsic is built — a hard failure on an
        older SDK where the old behaviour would at least still commit.
        """
        kwargs: dict = {}
        if block_time is not None and self._supports_block_time():
            kwargs["block_time"] = block_time
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
                    **kwargs,
                )
                self._consecutive_failures = 0
            except Exception:
                self._consecutive_failures += 1
                raise
        return self._parse_chain_result(result, "set_weights")

    def _supports_block_time(self) -> bool:
        """Does the installed SDK's ``set_weights`` accept ``block_time``?

        Cached, and any failure to introspect answers "no" — losing control
        of the reveal round costs us the fix, while passing a keyword the
        callee does not take costs us the commit entirely.
        """
        cached = getattr(self, "_block_time_supported", None)
        if cached is not None:
            return cached
        supported = False
        try:
            import inspect

            params = inspect.signature(self.subtensor.set_weights).parameters
            supported = "block_time" in params or any(
                p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
            )
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not introspect set_weights: {e}")
        if not supported:
            logger.warning(
                "This bittensor SDK's set_weights takes no block_time — the "
                "reveal round cannot be steered and commits will target ~36s "
                "past the epoch boundary. Upgrade the SDK to fix reveal timing."
            )
        self._block_time_supported = supported
        return supported

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

    async def get_last_epoch_block(self) -> int | None:
        """``LastEpochBlock`` for this subnet: the block the last epoch ran at.

        THIS IS THE ONLY HONEST EPOCH CLOCK, and the reason a second one is
        not used: `block // tempo` is a lattice anchored at block 0, and the
        chain's boundary is a stateful counter that has drifted off it — on
        mainnet the two were 3,329 blocks (~11h) apart when this was written.
        Anything that schedules against the lattice fires at an arbitrary
        phase relative to the real epoch.

        Deliberately NOT ``LastMechansimStepBlock`` (upstream's typo, and it
        is a different quantity): that is written only when an epoch actually
        distributed emissions, while ``LastEpochBlock`` advances whenever the
        slot is consumed — including a skipped epoch. Commits are keyed by
        the epoch counter that moves with ``LastEpochBlock``, so anchoring on
        the other one would leave us not committing while the queue key had
        already advanced, and the un-revealed commit would then be discarded.

        ``None`` on any read failure: absence of an answer is not an answer,
        and the caller falls back rather than treating it as a boundary.
        """
        try:
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                result = self.subtensor.substrate.query(
                    "SubtensorModule", "LastEpochBlock", [self.netuid]
                )
                if asyncio.iscoroutine(result):
                    result = await result
            value = getattr(result, "value", None)
            if value is None:
                logger.warning("LastEpochBlock read returned no value")
                return None
            self._consecutive_failures = 0
            return int(value)
        except Exception as e:
            self._consecutive_failures += 1
            logger.warning(f"Could not read LastEpochBlock: {e}")
            return None

    async def get_own_weights(self) -> "list[tuple[int, int]] | None":
        """This validator's weight vector as the chain holds it right now.

        Read from `Weights[netuid, uid]` storage, not from a lite metagraph
        (which carries no weights). Used to RE-SEND the last good vector when
        the epoch's traffic data cannot be read: the alternative that shipped
        for months was to give every unit of weight to the burn address,
        which zeroes the validator's trust and dividends for the epoch.

        Three outcomes and the caller must keep them apart: a vector; an
        empty list (this hotkey has never set weights — nothing to re-send);
        ``None`` (the chain could not be read — say nothing, do nothing).
        """
        try:
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                uid = self.subtensor.substrate.query(
                    "SubtensorModule", "Uids", [self.netuid, self.hotkey.ss58_address]
                )
                if asyncio.iscoroutine(uid):
                    uid = await uid
                uid_value = getattr(uid, "value", uid)
                if uid_value is None:
                    logger.warning("Own uid not found on chain; cannot read weights")
                    return None
                result = self.subtensor.substrate.query(
                    "SubtensorModule", "Weights", [self.netuid, int(uid_value)]
                )
                if asyncio.iscoroutine(result):
                    result = await result
            value = getattr(result, "value", result)
            self._consecutive_failures = 0
            if not value:
                return []
            return [(int(dest), int(weight)) for dest, weight in value]
        except Exception as e:
            self._consecutive_failures += 1
            logger.warning(f"Could not read own weights from chain: {e}")
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

    async def verify_weights_set(
        self, since_block: int, uids: list[int], weights: list[int]
    ) -> dict | None:
        """Read ``Weights`` back from chain: is OUR plain set_weights there?

        The proof for the commit-reveal-OFF path. A plain ``set_weights``
        writes two things in its block, and both are checked: the vector in
        ``Weights[netuid, uid]`` must equal what was submitted, u16 for u16,
        and ``LastUpdate[uid]`` must be at or after ``since_block``. The
        second half is what makes this a proof of THIS submit: a re-send of
        the vector already on chain matches ``Weights`` before it is even
        sent, and only ``LastUpdate`` moving says the chain accepted it.

        Same three outcomes as ``verify_weight_commit_landed``: landed / not
        landed / ``None`` when the chain could not be read.
        """
        try:
            async with self._subtensor_lock:
                await self._maybe_reconnect()
                uid_res = self.subtensor.substrate.query(
                    "SubtensorModule", "Uids", [self.netuid, self.hotkey.ss58_address]
                )
                if asyncio.iscoroutine(uid_res):
                    uid_res = await uid_res
                uid_value = getattr(uid_res, "value", uid_res)
                if uid_value is None:
                    logger.warning("Own uid not found on chain; cannot verify weights")
                    return None
                uid = int(uid_value)
                weights_res = self.subtensor.substrate.query(
                    "SubtensorModule", "Weights", [self.netuid, uid]
                )
                if asyncio.iscoroutine(weights_res):
                    weights_res = await weights_res
                last_update_res = self.subtensor.substrate.query(
                    "SubtensorModule", "LastUpdate", [self.netuid]
                )
                if asyncio.iscoroutine(last_update_res):
                    last_update_res = await last_update_res
            on_chain = getattr(weights_res, "value", weights_res) or []
            on_chain = sorted((int(d), int(w)) for d, w in on_chain)
            submitted = sorted((int(d), int(w)) for d, w in zip(uids, weights))
            last_update_all = getattr(last_update_res, "value", last_update_res) or []
            last_update = (
                int(last_update_all[uid]) if uid < len(last_update_all) else None
            )
            self._consecutive_failures = 0
            if last_update is not None and last_update >= since_block:
                if on_chain == submitted:
                    return {
                        "landed": True,
                        "set_block": last_update,
                        "uid": uid,
                        "n": len(on_chain),
                    }
                # The chain accepted a set of ours since the submit but holds
                # a different vector: say exactly how it differs, because the
                # retry this triggers will most likely be rate-limited and
                # the operator needs to know what is actually on chain.
                logger.error(
                    f"Weights on chain (LastUpdate={last_update}) differ from the "
                    f"vector submitted: on_chain={on_chain} submitted={submitted}"
                )
            return {
                "landed": False,
                "since_block": since_block,
                "last_update": last_update,
                "matches": on_chain == submitted,
            }
        except Exception as e:
            self._consecutive_failures += 1
            logger.warning(f"Could not verify Weights on chain: {e}")
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
