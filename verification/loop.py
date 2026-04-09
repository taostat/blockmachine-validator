"""
Verification loop — runs independently from weight setting.

Polls the registry for finalized epochs, fetches logs via manifest,
samples miner responses, verifies against reference nodes, bans on failure.
No scoring, no aggregation.
"""

import asyncio
import logging

from validator.common.types import MinerInfo, MinerVerificationState, QueryLog
from validator.config import VerificationConfig
from validator.metrics import (
    record_verification_ban,
    record_verification_epoch,
    record_verification_result,
)
from validator.protocols import (
    BlacklistService,
    ChainInterface,
    LogsSource,
    VerificationStore,
)
from validator.verification.blacklist import BlacklistManager
from validator.verification.logged import LoggedVerifier
from validator.verification.reference import ReferenceNodeManager

logger = logging.getLogger(__name__)

_MAX_STARTUP_EPOCHS = 10
_MAX_VERIFIED_TRACKED = 200


class VerificationLoop:
    def __init__(
        self,
        config: VerificationConfig,
        logs: LogsSource,
        chain: ChainInterface,
        blacklist: BlacklistService,
        store: VerificationStore,
        reference_manager: ReferenceNodeManager,
        poll_interval: int = 60,
    ):
        self.config = config
        self.logs = logs
        self.chain = chain
        self.blacklist = blacklist
        self.store = store
        self.reference_manager = reference_manager
        self.poll_interval = poll_interval

        validator_hotkey = chain.get_validator_hotkey()
        self.blacklist_manager = BlacklistManager(store, blacklist, validator_hotkey)
        self.logged_verifier = LoggedVerifier(config, reference_manager)

        self.running = False
        self._verified_epochs: set[str] = set()
        self._reference_healthy = True

    async def run(self):
        self.running = True
        logger.info("Verification loop started")

        await self._load_verified_epochs_from_store()

        while self.running:
            try:
                await self._process_available_epochs()
            except Exception as e:
                logger.error(f"Error in verification loop: {e}")

            await asyncio.sleep(self.poll_interval)

        logger.info("Verification loop stopped")

    async def stop(self):
        self.running = False

    async def _load_verified_epochs_from_store(self):
        try:
            finalized = await self.logs.fetch_finalized_epochs(
                limit=_MAX_STARTUP_EPOCHS
            )
            if not finalized:
                return

            for ep in finalized:
                epoch_id = ep.get("epoch_id")
                if epoch_id and await self.store.is_epoch_processed(epoch_id):
                    self._verified_epochs.add(epoch_id)

            if self._verified_epochs:
                logger.info(
                    f"Loaded {len(self._verified_epochs)} already-verified epochs from store"
                )
        except Exception as e:
            logger.warning(f"Could not load verified epochs from store: {e}")

    async def _process_available_epochs(self):
        finalized = await self.logs.fetch_finalized_epochs(limit=_MAX_STARTUP_EPOCHS)
        if not finalized:
            return

        for ep in finalized:
            epoch_id = ep.get("epoch_id")
            if not epoch_id or epoch_id in self._verified_epochs:
                continue
            manifest = ep.get("manifest")
            success = await self._verify_epoch(epoch_id, manifest)
            if success:
                self._verified_epochs.add(epoch_id)
                self._prune_verified()

    def _prune_verified(self):
        if len(self._verified_epochs) <= _MAX_VERIFIED_TRACKED:
            return
        try:
            sorted_epochs = sorted(self._verified_epochs, key=lambda x: int(x))
        except (ValueError, TypeError):
            sorted_epochs = sorted(self._verified_epochs)
        to_remove = len(self._verified_epochs) - _MAX_VERIFIED_TRACKED
        for eid in sorted_epochs[:to_remove]:
            self._verified_epochs.discard(eid)

    async def _verify_epoch(self, epoch_id: str, manifest: dict | None = None) -> bool:
        logger.info(f"[verification] processing epoch {epoch_id}")

        if not manifest:
            logger.warning(f"[verification] no manifest for epoch {epoch_id}, skipping")
            return True

        epoch_logs = await self.logs.fetch_epoch_logs_from_manifest(manifest, epoch_id)
        if not epoch_logs:
            logger.info(f"[verification] no logs for epoch {epoch_id}, skipping")
            return True

        miners = await self.chain.get_miners()

        for miner in miners:
            if await self.blacklist.is_blacklisted(miner.coldkey):
                continue
            try:
                await self._verify_miner(miner, epoch_id, epoch_logs)
            except Exception as e:
                logger.error(
                    f"[verification] error verifying miner "
                    f"{miner.hotkey[:20]}... in epoch {epoch_id}: {e}"
                )
                return False

        record_verification_epoch()
        logger.info(f"[verification] epoch {epoch_id} complete")
        return True

    async def _verify_miner(
        self, miner: MinerInfo, epoch_id: str, logs: list[QueryLog]
    ):
        if await self.blacklist_manager.is_blacklisted(miner.coldkey):
            return

        try:
            await self.store.ensure_miner(miner.hotkey, miner.coldkey, miner.uid)
        except Exception as e:
            logger.warning(f"[db] could not upsert miner {miner.hotkey[:20]}...: {e}")

        try:
            state = await self.store.get_verification_state(miner.hotkey)
        except Exception as e:
            logger.warning(
                f"[db] could not load verification state for {miner.hotkey[:20]}...: {e}"
            )
            state = None

        if state is None:
            state = MinerVerificationState(
                miner_hotkey=miner.hotkey,
                first_seen_epoch=epoch_id,
            )
            try:
                await self.store.save_verification_state(state)
            except Exception as e:
                logger.warning(
                    f"[db] could not save new verification state for {miner.hotkey[:20]}...: {e}"
                )

        miner_logs = [log for log in logs if log.miner_hotkey == miner.hotkey]
        if not miner_logs:
            return

        state.total_logged_queries += len(miner_logs)
        try:
            await self.store.save_verification_state(state)
        except Exception as e:
            logger.warning(
                f"[db] could not update query count for {miner.hotkey[:20]}...: {e}"
            )

        results = await self.logged_verifier.verify(
            miner,
            logs,
            sample_pct=self.config.logged_sample_pct,
            max_samples=self.config.logged_max_samples_per_miner,
        )

        for result in results:
            try:
                await self.store.save_verification_result(
                    result=result,
                    miner_hotkey=miner.hotkey,
                    node_id=result.node_id or "",
                    chain=result.chain or "",
                    epoch_id=epoch_id,
                )
            except Exception as e:
                logger.warning(
                    f"[db] could not save verification result for {miner.hotkey[:20]}... "
                    f"(method={result.method}, correct={result.is_correct}): {e}"
                )

            chain_label = result.chain or "unknown"
            record_verification_result(chain_label, result.is_correct)

            if result.is_correct:
                try:
                    await self.store.increment_pass_count(miner.hotkey)
                except Exception as e:
                    logger.warning(
                        f"[db] could not increment pass count for {miner.hotkey[:20]}...: {e}"
                    )
            else:
                try:
                    await self.store.increment_fail_count(miner.hotkey)
                except Exception as e:
                    logger.warning(
                        f"[db] could not increment fail count for {miner.hotkey[:20]}...: {e}"
                    )
                record_verification_ban()
                # Propagate DB errors from ban — a failed ban means a cheating
                # miner evades punishment.
                await self.blacklist_manager.handle_verification_failure(
                    miner, result, epoch_id
                )
                return
