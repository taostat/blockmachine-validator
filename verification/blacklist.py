import json
import logging

from validator.common.types import Incident, MinerInfo, VerificationResult
from validator.protocols import BlacklistService, VerificationStore

logger = logging.getLogger(__name__)

MAX_EVIDENCE_SIZE = 500000


class BlacklistManager:
    """
    A single incorrect response results in permanent coldkey ban.
    No recovery — the coldkey can never participate in the subnet again.
    """

    def __init__(
        self,
        store: VerificationStore,
        blacklist: BlacklistService,
        validator_hotkey: str,
    ):
        self.store = store
        self.blacklist = blacklist
        self.validator_hotkey = validator_hotkey

    async def is_blacklisted(self, coldkey: str) -> bool:
        return await self.blacklist.is_blacklisted(coldkey)

    async def handle_verification_failure(
        self, miner: MinerInfo, result: VerificationResult, epoch_id: str
    ):
        logger.warning(
            f"VERIFICATION FAILURE - PERMANENT BAN for miner {miner.hotkey} "
            f"(coldkey: {miner.coldkey}): {result.method}"
        )

        await self.store.save_incident(
            Incident(
                miner_hotkey=miner.hotkey,
                miner_coldkey=miner.coldkey,
                epoch_id=epoch_id,
            )
        )

        evidence = {
            "query": {
                "method": result.method,
                "params": result.params,
                "block_number": result.block_number,
                "chain": result.chain,
            },
            "miner_response_hash": result.miner_response_hash,
            "ref_response_hash": result.ref_response_hash,
            "ref_response": result.ref_response,
            "node_id": result.node_id,
            "source_query_id": result.source_query_id,
            "latency_ref_ms": result.latency_ref_ms,
            # Debug aids: per-candidate trace from the block-tolerance path.
            "used_block_tolerance": result.used_block_tolerance,
            "tolerance_attempts": result.tolerance_attempts,
            "validator_hotkey": self.validator_hotkey,
        }

        serialized = json.dumps(evidence, default=str)
        if len(serialized) > MAX_EVIDENCE_SIZE:
            evidence = {
                "_truncated": True,
                "query": {
                    "method": result.method,
                    "params": result.params,
                    "block_number": result.block_number,
                    "chain": result.chain,
                },
                "miner_response_hash": result.miner_response_hash,
                "ref_response_hash": result.ref_response_hash,
                "node_id": result.node_id,
                "source_query_id": result.source_query_id,
                "used_block_tolerance": result.used_block_tolerance,
                "tolerance_attempts": result.tolerance_attempts,
                "validator_hotkey": self.validator_hotkey,
            }

        await self.blacklist.ban(
            coldkey=miner.coldkey,
            hotkey=miner.hotkey,
            reason="Returned incorrect data for deterministic query - PERMANENT BAN",
            epoch_id=epoch_id,
            evidence=evidence,
        )

        logger.error(
            f"PERMANENT BAN APPLIED: coldkey={miner.coldkey}, hotkey={miner.hotkey}. "
            f"Method: {result.method}, block: {result.block_number}."
        )
