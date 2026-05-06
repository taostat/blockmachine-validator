import logging

from validator.common.scoring.weights import normalize_weights
from validator.config import WeightConfig
from validator.protocols import ChainInterface

logger = logging.getLogger(__name__)


class WeightSubmitter:
    def __init__(self, chain: ChainInterface, weights_config: WeightConfig):
        self.chain = chain
        # Held by reference so registry-driven hot-reloads of burn_sink_uid
        # take effect on the next submit() without a process restart.
        self._weights = weights_config

    async def submit(
        self,
        miner_weights: list[tuple[int, float]],
        burn_weight: float,
    ) -> bool:
        """Normalize and submit weights to chain. Returns True on success."""
        burn_sink_uid = self._weights.burn_sink_uid
        all_weights = list(miner_weights)
        if burn_weight > 0:
            all_weights.append((burn_sink_uid, burn_weight))

        normalized = normalize_weights(all_weights, burn_sink_uid)

        if not normalized:
            logger.warning("No weights to submit")
            return False

        uids = [uid for uid, _ in normalized]
        values = [val for _, val in normalized]

        logger.info(f"Submitting weights for {len(uids)} UIDs")
        for uid, w in zip(uids, values):
            label = " (burn)" if uid == burn_sink_uid else ""
            logger.info(f"  UID {uid}: {w}{label}")

        try:
            success = await self.chain.set_weights(uids, values)
            if success:
                logger.info("Weights submitted successfully")
            else:
                logger.error("Weight submission returned False")
            return success
        except Exception as e:
            logger.error(f"Weight submission error: {e}")
            return False

    async def blocks_until_next_epoch(self) -> int:
        block = await self.chain.get_current_block()
        tempo = await self.chain.get_tempo()
        if tempo == 0:
            return 0
        return tempo - (block % tempo)
