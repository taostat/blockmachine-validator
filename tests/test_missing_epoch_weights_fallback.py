"""Re-send the previous vector only when finalization misses an epoch."""

import asyncio
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock

if "bittensor" not in sys.modules:
    bt_stub = types.ModuleType("bittensor")
    bt_stub.Keypair = type("Keypair", (), {})
    sys.modules["bittensor"] = bt_stub

_REPO = Path(__file__).resolve().parents[1]
try:
    import validator  # noqa: F401
except ModuleNotFoundError:
    spec = importlib.util.spec_from_file_location(
        "validator",
        _REPO / "__init__.py",
        submodule_search_locations=[str(_REPO)],
    )
    assert spec and spec.loader, "could not bootstrap validator package"
    mod = importlib.util.module_from_spec(spec)
    sys.modules["validator"] = mod
    spec.loader.exec_module(mod)

from validator.common.types import MinerInfo
from validator.config import ValidatorConfig
from validator.weights.loop import WeightLoop

TEMPO = 7200
FALLBACK_BLOCK = 7100
EPOCH_START = 9_100_000
PREVIOUS = [(178, 65535), (129, 46026), (237, 21928)]


def run(coro):
    return asyncio.run(coro)


class FallbackChain:
    def __init__(self, *, blocks_into_epoch, last_update_block):
        self.epoch_start = EPOCH_START
        self.current_block = EPOCH_START + blocks_into_epoch
        self.last_update_block = last_update_block

    async def get_current_block(self):
        return self.current_block

    async def get_tempo(self):
        return TEMPO

    async def get_last_epoch_block(self):
        return self.epoch_start

    async def get_validator_last_update_block(self):
        return self.last_update_block

    async def get_own_weights(self):
        return list(PREVIOUS)

    async def commit_reveal_enabled(self):
        return True

    async def get_miners(self):
        return [MinerInfo(uid=44, hotkey="miner", coldkey="coldkey")]


def make_loop(*, blocks_into_epoch, last_update_block):
    chain = FallbackChain(
        blocks_into_epoch=blocks_into_epoch,
        last_update_block=last_update_block,
    )
    logs = types.SimpleNamespace(
        fetch_finalized_epochs=AsyncMock(return_value=None),
        fetch_cu_allocations_from_manifest=AsyncMock(
            return_value={"miner": {"total": 100, "non_archive": 100}}
        ),
    )
    store = types.SimpleNamespace(
        is_epoch_processed=AsyncMock(return_value=False),
        save_epoch_audit=AsyncMock(),
        mark_epoch_processed=AsyncMock(),
    )

    async def successful_resubmit(vector, block_time=None):
        chain.last_update_block = chain.current_block
        return True

    submitter = types.SimpleNamespace(
        resubmit_previous=AsyncMock(side_effect=successful_resubmit),
        submit=AsyncMock(return_value=True),
        last_commit_block=None,
    )
    loop = WeightLoop(
        config=ValidatorConfig(),
        chain=chain,
        logs=logs,
        blacklist=types.SimpleNamespace(
            get_banned_coldkeys=AsyncMock(return_value=set())
        ),
        store=store,
        prices=AsyncMock(),
        submitter=submitter,
    )
    loop._reveal_offset_blocks = 3
    return loop


def test_fallback_does_not_run_before_block_7100_of_the_epoch():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK - 1,
        last_update_block=EPOCH_START - 1,
    )

    run(loop._tick())

    loop.submitter.resubmit_previous.assert_not_awaited()


def test_fallback_at_block_7100_resends_the_previous_vector_unchanged():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START - 1,
    )

    run(loop._tick())

    loop.submitter.resubmit_previous.assert_awaited_once()
    assert loop.submitter.resubmit_previous.await_args.args[0] == PREVIOUS
    loop.submitter.submit.assert_not_awaited()
    assert loop._fallback_completed_epoch_block == EPOCH_START


def test_successful_fallback_is_not_retried_this_epoch():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START - 1,
    )

    run(loop._tick())
    loop.chain.current_block += 5
    run(loop._tick())

    loop.submitter.resubmit_previous.assert_awaited_once()


def test_fallback_is_skipped_if_weights_were_set_this_epoch():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START + 123,
    )

    run(loop._tick())

    loop.submitter.resubmit_previous.assert_not_awaited()


def test_failed_previous_weights_read_is_retried_on_a_later_tick():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START - 1,
    )
    loop.chain.get_own_weights = AsyncMock(
        side_effect=[RuntimeError("temporary RPC failure"), PREVIOUS]
    )

    run(loop._tick())
    loop.submitter.resubmit_previous.assert_not_awaited()
    assert loop._fallback_completed_epoch_block is None

    loop.chain.current_block += 5
    run(loop._tick())

    loop.submitter.resubmit_previous.assert_awaited_once()
    assert loop._fallback_completed_epoch_block == EPOCH_START


def test_rejected_resend_is_retried_on_a_later_tick():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START - 1,
    )
    attempts = 0

    async def reject_then_accept(vector, block_time=None):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return False
        loop.chain.last_update_block = loop.chain.current_block
        return True

    loop.submitter.resubmit_previous = AsyncMock(side_effect=reject_then_accept)

    run(loop._tick())
    loop.submitter.resubmit_previous.assert_awaited_once()
    assert loop._fallback_completed_epoch_block is None

    # A later 60-second loop tick is about five blocks later. The last
    # successful update is still more than 100 blocks old, so the chain's
    # rate limit does not block this retry.
    loop.chain.current_block += 5
    run(loop._tick())

    assert loop.submitter.resubmit_previous.await_count == 2
    assert loop._fallback_completed_epoch_block == EPOCH_START


def test_late_finalized_epoch_still_uses_normal_processing_after_fallback():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START - 1,
    )
    run(loop._tick())
    loop.store.mark_epoch_processed.assert_not_awaited()
    loop.logs.fetch_finalized_epochs.return_value = [
        {
            "epoch_id": "late-finalized-epoch",
            "miner_configs": {"miner": {}},
            "manifest": {"gateway": {}},
            "price_snapshot": {
                "alpha_price_usd": 1.0,
                "tao_price_usd": 1.0,
                "emissions_alpha": 100.0,
            },
        }
    ]
    run(loop._tick())

    loop.submitter.resubmit_previous.assert_awaited_once()
    loop.submitter.submit.assert_awaited_once()
    loop.store.mark_epoch_processed.assert_awaited_once_with(
        "late-finalized-epoch", weights_submitted=True
    )


def test_fallback_is_attempted_at_most_once_in_each_epoch():
    loop = make_loop(
        blocks_into_epoch=FALLBACK_BLOCK,
        last_update_block=EPOCH_START - 1,
    )

    run(loop._tick())
    run(loop._tick())
    loop.submitter.resubmit_previous.assert_awaited_once()

    loop.chain.epoch_start += TEMPO
    loop.chain.current_block = loop.chain.epoch_start + FALLBACK_BLOCK
    run(loop._tick())

    assert loop.submitter.resubmit_previous.await_count == 2
