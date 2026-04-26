"""
1. Uses method registry to determine which methods are verifiable
2. Validates that block-pinnable methods have explicit block parameters
3. For "latest" queries (missing block param but block_number logged by gateway),
   verifies with a 1-block window (N, N-1, and N+1) to handle race conditions
4. Resolves block hashes when needed (substrate methods take hashes, not numbers)
5. Filters out non-verifiable methods early
"""

import asyncio
import logging
import random
import time

from validator.common.types import (
    MinerInfo,
    QueryLog,
    VerificationResult,
    hash_response,
    hashes_match,
)
from validator.config import VerificationConfig
from validator.metrics import (
    record_reference_failure,
    record_verification_sample,
    record_verification_skipped,
)
from validator.verification.reference import ReferenceNodeManager
from validator.verification.method_registry import (
    BlockParamType,
    is_verifiable,
    requires_block_param,
    get_block_param_type,
)

logger = logging.getLogger(__name__)


class LoggedVerifier:
    """
    Verifies miners by sampling logged queries from the epoch
    and comparing response hashes against reference nodes.

    Only verifies methods marked as verifiable in the method registry.
    """

    def __init__(
        self,
        config: VerificationConfig,
        reference_manager: ReferenceNodeManager,
    ):
        self.config = config
        self.reference_manager = reference_manager
        self._consecutive_ref_failures = 0
        self._ref_failure_threshold = 20

    async def verify(
        self,
        miner: MinerInfo,
        logs: list[QueryLog],
        sample_pct: float,
        max_samples: int,
    ) -> list[VerificationResult]:
        miner_logs = [log for log in logs if log.miner_hotkey == miner.hotkey]

        verifiable_logs = []
        skipped_non_verifiable = 0
        skipped_missing_block_param = 0
        skipped_no_response_hash = 0
        skipped_unknown_chain = 0
        skipped_non_verifiable_methods: set[str] = set()

        for log in miner_logs:
            if log.status_code != 200 or not log.response_hash:
                skipped_no_response_hash += 1
                continue

            if not log.chain or log.chain.upper() == "UNKNOWN":
                skipped_unknown_chain += 1
                skipped_non_verifiable += 1
                continue

            if not is_verifiable(log.chain, log.method):
                skipped_non_verifiable_methods.add(log.method)
                skipped_non_verifiable += 1
                continue

            needs_block, param_name, param_index = requires_block_param(
                log.chain, log.method
            )

            if needs_block:
                params = self._extract_params(log)

                has_explicit_block_param = (
                    param_index < len(params) and params[param_index] is not None
                )

                if not has_explicit_block_param:
                    if log.inferred_from_latest or log.block_number:
                        verifiable_logs.append(log)
                        continue
                    else:
                        logger.warning(
                            f"Method {log.method} requires block param "
                            f"at index {param_index} but param is missing and no "
                            f"inferred block_number in log. Query ID: {log.id}"
                        )
                        skipped_missing_block_param += 1
                        continue

            verifiable_logs.append(log)

        if skipped_no_response_hash:
            record_verification_skipped("no_response_hash", skipped_no_response_hash)
        if skipped_non_verifiable:
            record_verification_skipped("non_verifiable_method", skipped_non_verifiable)
        if skipped_missing_block_param:
            record_verification_skipped(
                "missing_block_param", skipped_missing_block_param
            )

        non_verifiable_detail = ""
        if skipped_unknown_chain:
            non_verifiable_detail += f" ({skipped_unknown_chain} unknown chain — gateway missing GATEWAY_CHAIN?)"
        if skipped_non_verifiable_methods:
            top = sorted(skipped_non_verifiable_methods)[:5]
            sample = ", ".join(top)
            if len(skipped_non_verifiable_methods) > 5:
                sample += f" +{len(skipped_non_verifiable_methods) - 5} more"
            non_verifiable_detail += f" (normal non-verifiable: {sample})"

        logger.info(
            f"Filtered logs for {miner.hotkey[:20]}...: "
            f"{len(verifiable_logs)} verifiable, "
            f"{skipped_non_verifiable} non-verifiable{non_verifiable_detail}, "
            f"{skipped_missing_block_param} missing block params, "
            f"{skipped_no_response_hash} no response hash"
        )

        if not verifiable_logs:
            logger.info(
                f"No verifiable queries for {miner.hotkey[:20]}... "
                f"(had {len(miner_logs)} total logs)"
            )
            return []

        sample_size = min(max_samples, max(1, int(len(verifiable_logs) * sample_pct)))
        sampled_logs = random.sample(
            verifiable_logs, min(sample_size, len(verifiable_logs))
        )

        record_verification_sample(len(sampled_logs))

        results = []
        consecutive_errors = 0
        max_consecutive_errors = 5

        for log in sampled_logs:
            try:
                result = await self._verify_logged_query(log)
                results.append(result)

                if not result.is_correct:
                    logger.warning(
                        f"Verification FAIL: {log.method} (chain={log.chain}) "
                        f"miner={miner.hotkey[:20]}... "
                        f"miner_hash={log.response_hash[:16] if log.response_hash else 'None'}... "
                        f"ref_hash={result.ref_response_hash[:16] if result.ref_response_hash else 'None'}..."
                    )

                consecutive_errors = 0
                self._consecutive_ref_failures = 0
                await asyncio.sleep(0.1)

            except Exception as e:
                consecutive_errors += 1
                record_reference_failure()
                logger.warning(
                    f"Unhandled verification error for {miner.hotkey[:20]}... "
                    f"method={log.method} ({consecutive_errors}/{max_consecutive_errors}): {e}"
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        f"Too many consecutive verification errors for {miner.hotkey[:20]}..., "
                        f"stopping verification. Reference node may be unavailable."
                    )
                    break

        passed = sum(1 for r in results if r.is_correct)
        failed = len(results) - passed
        logger.info(
            f"Verification complete for {miner.hotkey[:20]}...: "
            f"{passed} passed, {failed} failed"
        )

        return results

    def _extract_params(self, log: QueryLog) -> list:
        if log.params is None:
            return []

        if isinstance(log.params, dict):
            return log.params.get("params", [])

        if isinstance(log.params, list):
            return log.params

        logger.warning(f"Unexpected params format: {type(log.params)}")
        return []

    async def _verify_logged_query(self, log: QueryLog) -> VerificationResult:
        params = self._extract_params(log)

        needs_block, _, param_index = requires_block_param(log.chain, log.method)

        has_explicit_block_param = (
            needs_block
            and param_index < len(params)
            and params[param_index] is not None
        )

        if needs_block and not has_explicit_block_param and (
            log.inferred_from_latest or log.block_number
        ):
            return await self._verify_with_block_tolerance(log, params, param_index)

        return await self._verify_exact(log, params)

    async def _verify_exact(self, log: QueryLog, params: list) -> VerificationResult:
        try:
            ref_start = time.time()
            ref_response = await self.reference_manager.query(
                log.chain, log.method, params
            )
            ref_latency = int((time.time() - ref_start) * 1000)
            ref_response_hash = hash_response(ref_response, log.method)
            self._consecutive_ref_failures = 0

        except Exception as e:
            self._consecutive_ref_failures += 1
            if self._consecutive_ref_failures <= 3:
                logger.warning(
                    f"Reference query failed for {log.method} on {log.chain}: {e}. "
                    f"Marking as correct to avoid false positive."
                )
            elif self._consecutive_ref_failures == 4:
                logger.warning(
                    f"Reference node unhealthy (chain={log.chain}) — "
                    f"suppressing per-query warnings until it recovers"
                )
            return VerificationResult(
                is_correct=True,
                method=log.method,
                params=params,
                block_number=log.block_number,
                chain=log.chain,
                node_id=log.node_id,
                source_query_id=log.id,
                miner_response_hash=log.response_hash,
                error_details=f"Reference query failed: {e}",
            )

        is_correct = hashes_match(log.response_hash, ref_response_hash)

        return VerificationResult(
            is_correct=is_correct,
            method=log.method,
            params=params,
            block_number=log.block_number,
            chain=log.chain,
            node_id=log.node_id,
            source_query_id=log.id,
            miner_response_hash=log.response_hash,
            ref_response_hash=ref_response_hash,
            latency_ref_ms=ref_latency,
            ref_response=ref_response if not is_correct else None,
        )

    async def _verify_with_block_tolerance(
        self, log: QueryLog, params: list, block_param_index: int
    ) -> VerificationResult:
        block_number = log.block_number
        block_type = get_block_param_type(log.chain, log.method) or BlockParamType.HASH

        if block_number > 0:
            candidates = [block_number, block_number - 1, block_number + 1]
        else:
            candidates = [block_number]

        ref_errors = []
        last_ref_hash = None
        last_ref_latency = None
        last_ref_response = None

        for n in candidates:
            try:
                pinned_params = await self._build_pinned_params(
                    log.chain, params, block_param_index, n, block_type
                )

                ref_start = time.time()
                ref_response = await self.reference_manager.query(
                    log.chain, log.method, pinned_params
                )
                ref_latency = int((time.time() - ref_start) * 1000)

                ref_response_hash = hash_response(ref_response, log.method)

                if hashes_match(log.response_hash, ref_response_hash):
                    return VerificationResult(
                        is_correct=True,
                        method=log.method,
                        params=params,
                        block_number=n,
                        chain=log.chain,
                        node_id=log.node_id,
                        source_query_id=log.id,
                        miner_response_hash=log.response_hash,
                        ref_response_hash=ref_response_hash,
                        latency_ref_ms=ref_latency,
                    )

                last_ref_hash = ref_response_hash
                last_ref_latency = ref_latency
                last_ref_response = ref_response

            except Exception as e:
                ref_errors.append(f"block {n}: {e}")
                logger.warning(
                    f"Reference query failed for {log.method} at block {n} "
                    f"on {log.chain}: {e}"
                )
                continue

        if len(ref_errors) == len(candidates):
            self._consecutive_ref_failures += 1
            if self._consecutive_ref_failures <= 3:
                logger.warning(
                    f"All tolerance reference queries failed for {log.method} on "
                    f"{log.chain}: {ref_errors}. Marking correct to avoid false positive."
                )
            elif self._consecutive_ref_failures == 4:
                logger.warning(
                    f"Reference node unhealthy (chain={log.chain}) — "
                    f"suppressing per-query warnings until it recovers"
                )
            return VerificationResult(
                is_correct=True,
                method=log.method,
                params=params,
                block_number=block_number,
                chain=log.chain,
                node_id=log.node_id,
                source_query_id=log.id,
                miner_response_hash=log.response_hash,
                error_details=f"All reference queries failed: {ref_errors}",
            )

        return VerificationResult(
            is_correct=False,
            method=log.method,
            params=params,
            block_number=block_number,
            chain=log.chain,
            node_id=log.node_id,
            source_query_id=log.id,
            miner_response_hash=log.response_hash,
            ref_response_hash=last_ref_hash,
            latency_ref_ms=last_ref_latency,
            ref_response=last_ref_response,
        )

    async def _build_pinned_params(
        self,
        chain: str,
        original_params: list,
        block_param_index: int,
        block_number: int,
        block_type: BlockParamType,
    ) -> list:
        params = list(original_params)

        if block_type == BlockParamType.HASH:
            block_value = await self.reference_manager.get_block_hash(
                chain, block_number
            )
            if not block_value:
                raise ValueError(
                    f"Could not resolve block hash for {chain} block {block_number}"
                )
        else:
            block_value = hex(block_number)

        while len(params) <= block_param_index:
            params.append(None)

        params[block_param_index] = block_value
        return params
