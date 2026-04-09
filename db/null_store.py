import logging
from typing import Optional

from validator.common.types import (
    EpochAudit,
    Incident,
    MinerVerificationState,
    VerificationResult,
)

logger = logging.getLogger(__name__)


class NullStore:
    """
    In-memory implementation of EpochStore + VerificationStore.
    Used when PostgreSQL is not configured. Only tracks what's needed
    for the loops to function — no persistence across restarts.
    """

    def __init__(self):
        self._processed_epochs: dict[str, bool] = {}
        self._verification_states: dict[str, MinerVerificationState] = {}

    async def connect(self):
        logger.info("NullStore active — no database persistence")

    async def close(self):
        pass

    # --- EpochStore ---

    async def is_epoch_processed(self, epoch_id: str) -> bool:
        return epoch_id in self._processed_epochs

    async def mark_epoch_processed(
        self, epoch_id: str, weights_submitted: bool = False
    ):
        self._processed_epochs[epoch_id] = weights_submitted

    async def save_epoch_audit(self, audit: EpochAudit):
        logger.debug(f"NullStore: audit for {audit.epoch_id} (not persisted)")

    # --- VerificationStore ---

    async def ensure_miner(self, hotkey: str, coldkey: str, uid: int):
        pass

    async def get_verification_state(
        self, miner_hotkey: str
    ) -> Optional[MinerVerificationState]:
        return self._verification_states.get(miner_hotkey)

    async def save_verification_state(self, state: MinerVerificationState):
        self._verification_states[state.miner_hotkey] = state

    async def save_verification_result(
        self,
        result: VerificationResult,
        miner_hotkey: str,
        node_id: str,
        chain: str,
        epoch_id: str,
    ):
        pass

    async def save_incident(self, incident: Incident):
        logger.warning(
            f"NullStore: incident for {incident.miner_hotkey} not persisted "
            f"(epoch={incident.epoch_id})"
        )

    async def increment_pass_count(self, miner_hotkey: str):
        state = self._verification_states.get(miner_hotkey)
        if state:
            state.logged_pass_count += 1

    async def increment_fail_count(self, miner_hotkey: str):
        state = self._verification_states.get(miner_hotkey)
        if state:
            state.logged_fail_count += 1
