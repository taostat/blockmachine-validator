import asyncio
import logging
import time

from validator.common.types import EpochAudit, MinerEpochData, EpochWeightsResult
from validator.chain.submitter import WeightSubmitter
from validator.config import ValidatorConfig
from validator.metrics import (
    record_blocks_since_update,
    record_epoch,
    record_miner_epoch_data,
    record_weight_submission,
)
from validator.protocols import (
    BlacklistService,
    ChainInterface,
    EpochStore,
    LogsSource,
    PriceSource,
)
from validator.common.scoring.weights import compute_epoch_weights
from validator.weights.types import normalize_miner_config

logger = logging.getLogger(__name__)

_CU_RETRY_TIMEOUT = 1800  # 30 min


class WeightLoop:
    def __init__(
        self,
        config: ValidatorConfig,
        chain: ChainInterface,
        logs: LogsSource,
        blacklist: BlacklistService,
        store: EpochStore,
        prices: PriceSource,
        submitter: WeightSubmitter,
    ):
        self.config = config
        self.chain = chain
        self.logs = logs
        self.blacklist = blacklist
        self.store = store
        self.prices = prices
        self.submitter = submitter
        self.running = False
        self._cu_retry_start: dict[str, float] = {}
        self._last_submitted_block: int | None = None
        self._known_commit_hashes: set[str] = set()
        self._last_update_block: int | None = None
        self._session_commits: dict[str, dict] = {}
        self._commits_initialized = False

    async def run(self):
        self.running = True
        logger.info("Weight loop started")

        while self.running:
            try:
                await self._tick()
            except Exception as e:
                logger.error(f"Error in weight loop: {e}", exc_info=True)

            await asyncio.sleep(60)

        logger.info("Weight loop stopped")

    async def stop(self):
        self.running = False

    async def _tick(self):
        if hasattr(self.chain, "get_blocks_since_last_update"):
            blocks_ago = await self.chain.get_blocks_since_last_update()
            if blocks_ago is not None:
                logger.info(f"Blocks since last weight update: {blocks_ago}")
                record_blocks_since_update(blocks_ago)

        if hasattr(self.chain, "get_pending_weight_commits"):
            commits = await self.chain.get_pending_weight_commits()
            if commits is None:
                logger.debug("Could not fetch pending weight commits")
            else:
                current_hashes = {c["hash"] for c in commits}
                commits_by_hash = {c["hash"]: c for c in commits}

                # on first tick just record existing commits as baseline without claiming them
                if self._commits_initialized:
                    new_hashes = current_hashes - self._known_commit_hashes
                    for h in new_hashes:
                        self._session_commits[h] = commits_by_hash[h]
                        short = h[:24] if isinstance(h, str) else str(h)[:24]
                        logger.info(
                            f"Commit {short}... registered "
                            f"(committed at block {commits_by_hash[h]['commit_block']})"
                        )

                vanished = self._known_commit_hashes - current_hashes
                if vanished:
                    new_last_update = None
                    if hasattr(self.chain, "get_validator_last_update_block"):
                        new_last_update = (
                            await self.chain.get_validator_last_update_block()
                        )

                    revealed = (
                        new_last_update is not None
                        and self._last_update_block is not None
                        and new_last_update > self._last_update_block
                    )
                    for h in vanished:
                        if h not in self._session_commits:
                            continue
                        short = h[:24] if isinstance(h, str) else str(h)[:24]
                        if revealed:
                            logger.info(
                                f"Commit {short}... REVEALED successfully "
                                f"(last_update advanced to block {new_last_update})"
                            )
                        else:
                            logger.warning(
                                f"Commit {short}... DISAPPEARED without reveal "
                                f"(last_update={new_last_update}, previous={self._last_update_block}) "
                                f"— likely expired or chain rejected"
                            )
                        self._session_commits.pop(h, None)

                    if new_last_update is not None:
                        self._last_update_block = new_last_update

                if self._last_update_block is None and hasattr(
                    self.chain, "get_validator_last_update_block"
                ):
                    self._last_update_block = (
                        await self.chain.get_validator_last_update_block()
                    )

                self._known_commit_hashes = current_hashes
                self._commits_initialized = True

                if self._session_commits:
                    current_block = await self.chain.get_current_block()
                    for h, c in self._session_commits.items():
                        c = commits_by_hash.get(h, c)
                        blocks_until_reveal = c["first_reveal_block"] - current_block
                        blocks_until_expiry = c["last_reveal_block"] - current_block
                        short = h[:24] if isinstance(h, str) else str(h)[:24]
                        if blocks_until_reveal <= 0:
                            logger.info(
                                f"Commit {short}... ready to reveal | "
                                f"expires in {blocks_until_expiry} blocks"
                            )
                        else:
                            logger.info(
                                f"Commit {short}... reveals in {blocks_until_reveal} blocks | "
                                f"expires in {blocks_until_expiry} blocks"
                            )

        # tempo guard: don't submit more than once per tempo window
        if self._last_submitted_block is not None:
            current_block = await self.chain.get_current_block()
            tempo = await self.chain.get_tempo()
            if tempo > 0:
                last_window = self._last_submitted_block // tempo
                current_window = current_block // tempo
                if last_window == current_window:
                    blocks_left = tempo - (current_block % tempo)
                    logger.info(
                        f"Already submitted this tempo window, "
                        f"{blocks_left} blocks until next window (tempo={tempo})"
                    )
                    return

        latest = await self._find_latest_unprocessed_epoch()
        if latest is None:
            logger.info(
                "No unprocessed epochs found — waiting for next epoch to finalize"
            )
            return

        epoch_id, miner_configs, manifest, price_snapshot = latest
        await self._process_epoch(epoch_id, miner_configs, manifest, price_snapshot)

    async def _find_latest_unprocessed_epoch(self):
        """
        Find the single most recent finalized epoch that is ready to process.
        If a validator is behind, older unprocessed epochs are marked as skipped
        so all validators converge to the same epoch.
        """
        finalized = await self.logs.fetch_finalized_epochs(limit=20)

        if finalized is not None:
            if not finalized:
                logger.info("Registry returned 0 finalized epochs")
                return None

            epoch_ids = [ep.get("epoch_id") for ep in finalized if ep.get("epoch_id")]
            logger.info(
                f"Registry returned {len(finalized)} finalized epochs: {epoch_ids}"
            )

            for i, ep in enumerate(finalized):
                epoch_id = ep.get("epoch_id")
                if not epoch_id:
                    continue

                miner_configs = ep.get("miner_configs")
                manifest = ep.get("manifest")
                price_snapshot = ep.get("price_snapshot")

                if miner_configs is None:
                    miner_configs = await self.logs.get_miner_configs(epoch_id)
                if miner_configs is None:
                    logger.info(
                        f"Epoch {epoch_id}: no miner configs yet, trying older epoch"
                    )
                    continue

                # most recent ready epoch — if already processed we are up to date
                if await self.store.is_epoch_processed(epoch_id):
                    logger.info(f"Epoch {epoch_id}: already processed — up to date")
                    return None

                # validator is behind — mark older epochs as skipped (no weights submitted)
                # so we immediately align with the rest of the network on the latest epoch
                for older_ep in finalized[i + 1 :]:
                    older_id = older_ep.get("epoch_id")
                    if not older_id:
                        continue
                    if not await self.store.is_epoch_processed(older_id):
                        logger.warning(
                            f"EPOCH SKIPPED (catch-up): {older_id} — validator was behind, "
                            f"aligning to latest epoch {epoch_id}, no weights submitted for {older_id}"
                        )
                        await self.store.mark_epoch_processed(
                            older_id, weights_submitted=False
                        )

                return (epoch_id, miner_configs, manifest, price_snapshot)

            return None

        logger.error("Registry unavailable — cannot discover epochs")
        return None

    async def _process_epoch(
        self,
        epoch_id: str,
        miner_configs: dict,
        manifest: dict = None,
        price_snapshot: dict = None,
    ) -> bool:
        logger.info(f"Processing epoch {epoch_id} for weights")

        # Declared burn: epoch finalizer explicitly registered this epoch as
        # 100% burn (miner_configs={} means zero gateway activity). Skip the
        # 30-min CU retry and go straight to burn submission so all validators
        # converge on the same result without waiting.
        if isinstance(miner_configs, dict) and len(miner_configs) == 0:
            logger.info(
                f"Epoch {epoch_id}: declared 100% burn by epoch finalizer "
                f"(no gateway activity) — submitting burn directly"
            )
            submitted = await self._submit_direct([], 1.0)
            if submitted:
                self._last_submitted_block = await self.chain.get_current_block()
            await self.store.mark_epoch_processed(epoch_id, weights_submitted=submitted)
            return True

        if manifest:
            cu_allocations = await self.logs.fetch_cu_allocations_from_manifest(
                manifest
            )
        else:
            logger.warning(
                f"Epoch {epoch_id}: no manifest, cannot fetch CU allocations"
            )
            cu_allocations = {}

        if not cu_allocations:
            if epoch_id not in self._cu_retry_start:
                self._cu_retry_start[epoch_id] = time.time()

            elapsed = time.time() - self._cu_retry_start[epoch_id]
            if elapsed < _CU_RETRY_TIMEOUT:
                logger.warning(
                    f"No CU allocations for {epoch_id}, "
                    f"retrying ({elapsed:.0f}s / {_CU_RETRY_TIMEOUT}s)"
                )
                return False

            logger.warning(
                f"No CU allocations for {epoch_id} after {elapsed:.0f}s, submitting 100% burn"
            )
            self._cu_retry_start.pop(epoch_id, None)
            submitted = await self._submit_direct([], 1.0)
            if submitted:
                self._last_submitted_block = await self.chain.get_current_block()
            await self.store.mark_epoch_processed(epoch_id, weights_submitted=submitted)
            return True

        self._cu_retry_start.pop(epoch_id, None)

        miners = await self.chain.get_miners()
        coldkey_map = {m.hotkey: m.coldkey for m in miners}
        uid_map = {m.hotkey: m.uid for m in miners}

        orphaned = set(cu_allocations.keys()) - set(uid_map.keys())
        if orphaned:
            logger.warning(
                f"Epoch {epoch_id}: {len(orphaned)} miners in CU allocations "
                f"but not in metagraph (deregistered?), they will be skipped"
            )

        if price_snapshot:
            alpha_price = price_snapshot["alpha_price_usd"]
            tao_price = price_snapshot.get("tao_price_usd", 0.0)
            emissions = price_snapshot["emissions_alpha"]
            logger.info(
                f"Using registry price snapshot: α=${alpha_price:.4f}, "
                f"TAO=${tao_price:.2f}, emissions={emissions:.2f}"
            )
        else:
            try:
                alpha_price = await self.prices.get_alpha_price_usd()
                tao_price = await self.prices.get_tao_price_usd()
                emissions = await self.prices.get_emissions_alpha()
            except Exception as e:
                logger.error(f"Failed to fetch prices/emissions: {e}")
                fb_price = self.config.weights.fallback_alpha_price_usd
                fb_emissions = self.config.weights.fallback_emissions_alpha
                if fb_price and fb_emissions:
                    alpha_price = fb_price
                    emissions = fb_emissions
                    tao_price = 0.0
                    logger.info(
                        f"Using fallbacks: α=${alpha_price:.4f}, emissions={emissions:.2f}"
                    )
                else:
                    return False

        default_price = self.config.weights.default_target_usd_per_cu

        # fetch ban list before building miner data so banned miners are
        # excluded from consumed/scale/burn calculations entirely
        banned_coldkeys = await self.blacklist.get_banned_coldkeys()

        miner_data = []
        for hotkey, cu_data in cu_allocations.items():
            if hotkey not in uid_map:
                continue

            coldkey = coldkey_map.get(hotkey, "")
            is_banned = bool(coldkey and coldkey in banned_coldkeys)
            if is_banned:
                logger.info(f"Excluding banned miner from weights: {hotkey[:20]}...")

            cfg = miner_configs.get(hotkey)
            prices = normalize_miner_config(cfg, default_price)
            price_non_archive = prices["price_non_archive"]
            price_archive = prices["price_archive"]

            cu_total = cu_data["total"]
            cu_archive = cu_data.get("archive", 0)
            cu_non_archive = cu_data.get("non_archive", 0)

            miner_data.append(
                MinerEpochData(
                    hotkey=hotkey,
                    coldkey=coldkey,
                    cu_total=cu_total,
                    target_usd_per_cu=price_non_archive,
                    is_blacklisted=is_banned,
                    cu_archive=cu_archive,
                    cu_non_archive=cu_non_archive,
                    price_archive=price_archive,
                    price_non_archive=price_non_archive,
                )
            )

        weights_result = compute_epoch_weights(
            miner_data,
            alpha_price_usd=alpha_price,
            emissions_alpha=emissions,
        )

        # banned miners already have is_blacklisted=True → consumed=0, weight=0
        miner_weights = []
        for m in weights_result.miners:
            if m.miner_hotkey not in uid_map:
                continue
            if m.weight > 0:
                miner_weights.append((uid_map[m.miner_hotkey], m.weight))

        submitted = await self._submit_with_retry(
            miner_weights, weights_result.burn_weight
        )
        record_weight_submission("direct", submitted)

        await self._save_audit_and_mark(
            epoch_id,
            weights_result,
            alpha_price,
            tao_price,
            coldkey_map,
            uid_map,
            submitted,
        )
        return True

    async def _submit_direct(
        self,
        miner_weights: list[tuple[int, float]],
        burn_weight: float,
    ) -> bool:
        """direct submission (no retry, used for burn-only)"""
        return await self.submitter.submit(miner_weights, burn_weight)

    async def _save_audit_and_mark(
        self,
        epoch_id: str,
        weights_result: EpochWeightsResult,
        alpha_price: float,
        tao_price: float,
        coldkey_map: dict,
        uid_map: dict,
        submitted: bool,
    ):
        block = await self.chain.get_current_block()
        if submitted:
            self._last_submitted_block = block

        miners_paid = sum(1 for m in weights_result.miners if m.weight > 0)
        miners_banned = sum(1 for m in weights_result.miners if m.is_banned)

        await self.store.save_epoch_audit(
            EpochAudit(
                epoch_id=epoch_id,
                block_number=block,
                total_cu=weights_result.pool_total_cu,
                miners_paid=miners_paid,
                miners_banned=miners_banned,
                total_consumed_usd=weights_result.total_consumed_usd,
                alpha_price_usd=alpha_price,
                tao_price_usd=tao_price,
                emissions_alpha=weights_result.emissions_alpha,
                burn_pct=weights_result.burn_pct,
                weights_submitted=submitted,
                miner_details={
                    m.miner_hotkey: {
                        "cu": m.cu_total,
                        "price": m.target_usd_per_cu,
                        "consumed_usd": m.consumed_usd,
                        "payout_usd": m.payout_usd,
                        "weight": m.weight,
                        "banned": m.is_banned,
                    }
                    for m in weights_result.miners
                },
            )
        )

        record_epoch(
            total_cu=weights_result.pool_total_cu,
            miners_paid=miners_paid,
            miners_banned=miners_banned,
            burn_pct=weights_result.burn_pct,
            consumed_usd=weights_result.total_consumed_usd,
            alpha_price=alpha_price,
            tao_price=tao_price,
            emissions=weights_result.emissions_alpha,
            weights_submitted=submitted,
            epoch_id=epoch_id,
        )
        record_miner_epoch_data(
            weights_result.miners,
            uid_map,
            epoch_id=epoch_id,
            emissions_alpha=weights_result.emissions_alpha,
        )

        await self.store.mark_epoch_processed(epoch_id, weights_submitted=submitted)

        logger.info(
            f"Epoch {epoch_id} done — "
            f"α=${alpha_price:.4f}, consumed=${weights_result.total_consumed_usd:.6f}, "
            f"paid {miners_paid} miners, "
            f"burn={weights_result.burn_pct:.1%}, submitted={submitted}"
        )

        if submitted:
            active_miners = [
                m for m in weights_result.miners if m.weight > 0 and not m.is_banned
            ]
            if active_miners:
                logger.info(
                    f"Submission breakdown — TAO=${tao_price:.2f}, α=${alpha_price:.4f}, "
                    f"total emissions={weights_result.emissions_alpha:.2f}α"
                )
                for m in sorted(active_miners, key=lambda x: x.weight, reverse=True):
                    uid = uid_map.get(m.miner_hotkey, "?")
                    miner_alpha = m.weight * weights_result.emissions_alpha
                    logger.info(
                        f"  UID {uid} | cu={m.cu_total:.0f} | "
                        f"price=${m.target_usd_per_cu:.8f}/cu | "
                        f"consumed=${m.consumed_usd:.6f} | "
                        f"weight={m.weight:.8f} | "
                        f"α_emissions={miner_alpha:.4f}α | "
                        f"usd_paid=${m.payout_usd:.6f}"
                    )

    async def _submit_with_retry(
        self,
        miner_weights: list[tuple[int, float]],
        burn_weight: float,
        max_retries: int = 3,
    ) -> bool:
        for attempt in range(max_retries):
            success = await self.submitter.submit(miner_weights, burn_weight)
            if success:
                return True

            remaining = await self.submitter.blocks_until_next_epoch()
            if remaining < 5:
                logger.error(
                    f"Only {remaining} blocks left in epoch, giving up on retry"
                )
                return False

            wait = min(30, remaining * 6)
            logger.warning(
                f"Weight submission failed (attempt {attempt + 1}/{max_retries}), "
                f"{remaining} blocks remain, retrying in {wait}s"
            )
            await asyncio.sleep(wait)

        return False
