import asyncio
import logging

from validator.common.scoring.weights import normalize_weights
from validator.config import WeightConfig
from validator.protocols import ChainInterface

logger = logging.getLogger(__name__)


# Verification polling: the SDK submits with wait_for_finalization=True, so
# the first read normally already sees the commit; the retries only cover
# read lag. ~12s is one block.
_VERIFY_ATTEMPTS = 5
_VERIFY_DELAY_SECS = 12.0


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
            # Captured BEFORE the submit so the chain read below answers
            # "did THIS submit land", not "have we ever committed".
            since_block = await self.chain.get_current_block()
            claimed = await self.chain.set_weights(uids, values)
        except Exception as e:
            logger.error(f"Weight submission error: {e}")
            return False

        # The SDK's verdict is the submitter reporting on itself. The only
        # proof a commit exists is reading it back from chain storage —
        # every cause of non-arrival (rate-limit rejection, network error,
        # a lying return value) presents identically as "not in storage",
        # so no failure classification is needed. Storage is checked in
        # BOTH directions: a claimed rejection gets two reads (a real
        # rejection has nothing to wait for; the second read covers an
        # SDK false NEGATIVE whose commit is still propagating) so a
        # landed commit still marks a true success; a claimed success is
        # polled longer to cover inclusion-read lag. A miss that slips
        # both reads converges chain-side: the retry is either rejected
        # by the rate limit or lands an identical-content duplicate.
        #
        # A false "not submitted" verdict only costs a retry, and retries
        # are chain-bounded twice over: inside the WeightsSetRateLimit
        # window a duplicate is rejected outright, and landed duplicates
        # are capped at 10 unrevealed commits per hotkey per epoch with
        # identical-content vectors, of which the last-decrypted wins.
        attempts = _VERIFY_ATTEMPTS if claimed else 2
        verified = await self._commit_landed_on_chain(since_block, attempts)
        if verified:
            if not claimed:
                logger.warning(
                    "SDK reported a rejection but the commit IS on chain — "
                    "trusting storage over the SDK"
                )
            logger.info("Weights submitted successfully")
        elif not claimed:
            logger.error("Weight submission rejected — will retry")
        return verified

    async def _commit_landed_on_chain(self, since_block: int, attempts: int) -> bool:
        could_not_read = 0
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(_VERIFY_DELAY_SECS)
            try:
                proof = await self.chain.verify_weight_commit_landed(since_block)
            except Exception as e:
                # submit() promises bool; a verifier that raises is a
                # verifier that could not read.
                logger.warning(f"Commit verification raised: {e}")
                proof = None
            if not isinstance(proof, dict):
                could_not_read += 1
                continue
            if proof.get("landed"):
                logger.info(
                    f"Commit verified on chain: epoch={proof.get('epoch')} "
                    f"block={proof.get('commit_block')} "
                    f"reveal_round={proof.get('reveal_round')} "
                    f"ct_len={proof.get('ct_len')}"
                )
                return True
            # Definite absence: keep polling briefly for inclusion lag,
            # then fail so the loop retries.
        if could_not_read == attempts:
            logger.error(
                "Could not read the chain to verify the commit — treating "
                "as NOT submitted (fail-closed). A duplicate retry is "
                "harmless: the chain's rate limit rejects it if the "
                "original landed."
            )
        else:
            logger.error(
                f"SDK claimed success but no commit of ours is on chain "
                f"since block {since_block} — treating as NOT submitted "
                f"so the epoch stays unprocessed and is retried."
            )
        return False

    async def blocks_until_next_epoch(self) -> int:
        block = await self.chain.get_current_block()
        tempo = await self.chain.get_tempo()
        if tempo == 0:
            return 0
        return tempo - (block % tempo)
