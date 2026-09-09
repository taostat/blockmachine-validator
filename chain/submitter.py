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
        # The block the last verified commit was INCLUDED at, straight from
        # the storage read that proved it. Not the block we finished
        # verifying at: verification polls for up to a minute, so those two
        # can sit on opposite sides of an epoch boundary, and the caller
        # dedupes commits per epoch off this value.
        self.last_commit_block: int | None = None
        # Held by reference so registry-driven hot-reloads of burn_sink_uid
        # take effect on the next submit() without a process restart.
        self._weights = weights_config

    async def submit(
        self,
        miner_weights: list[tuple[int, float]],
        burn_weight: float,
        block_time: float | None = None,
    ) -> bool:
        """Normalize and submit weights to chain. Returns True on success.

        ``block_time`` steers which drand round the commit targets; the
        caller recomputes it per attempt from the remaining runway. ``None``
        leaves the SDK's default, which aims past the boundary.
        """
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
            # Passed only when we actually computed one: a ChainInterface
            # that predates this keyword (or a test double) must keep
            # working, and `None` carries no information the callee needs.
            extra = {} if block_time is None else {"block_time": block_time}
            claimed = await self.chain.set_weights(uids, values, **extra)
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
        self.last_commit_block = None
        verified = await self._commit_landed_on_chain(since_block, attempts)
        if verified and self.last_commit_block is None:
            # Verified but the proof carried no block: fall back to the block
            # sampled BEFORE the submit, which is never later than inclusion.
            # Recording a later block is what suppresses the next epoch's
            # commit; recording an earlier one only risks a duplicate that
            # the chain's own rate limit rejects.
            self.last_commit_block = since_block
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
                commit_block = proof.get("commit_block")
                self.last_commit_block = (
                    int(commit_block) if commit_block is not None else None
                )
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
        """Blocks until the chain's next epoch boundary.

        Anchored on ``LastEpochBlock``, not on ``block % tempo``: the lattice
        is anchored at block 0 and the chain's boundary is a stateful counter
        that has drifted off it, so the old arithmetic could report 200
        blocks left with 3,500 to go, or the reverse. Callers use this to
        decide whether there is time to retry, so being wrong here abandons
        submissions that had hours in hand.

        Falls back to the lattice only when the anchor cannot be read, and
        clamps at 0 so a caller never sees a negative runway.
        """
        block = await self.chain.get_current_block()
        tempo = await self.chain.get_tempo()
        if tempo <= 0:
            return 0
        last_epoch_block = None
        getter = getattr(self.chain, "get_last_epoch_block", None)
        if getter is not None:
            try:
                last_epoch_block = await getter()
            except Exception as e:
                logger.warning(f"Could not read the epoch anchor: {e}")
        if last_epoch_block is None:
            return tempo - (block % tempo)
        return max(0, last_epoch_block + tempo - block)
