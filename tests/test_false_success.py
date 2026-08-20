"""The 2026-08-20 false-success defect, pinned.

Our validator logged "Weights submitted successfully" for a commit the
chain rejected (87 blocks inside the 100-block WeightsSetRateLimit
window), marked the epoch processed, and never retried — observed live:
LastUpdate never moved, and the validator did not resubmit after the
floor cleared.

Two mechanisms, both pinned here:

1. bittensor's ExtrinsicResponse is tuple-LIKE (indexable, iterable,
   len() == 2 via __len__) but NOT a tuple, and defines no __bool__ —
   so `isinstance(result, tuple)` misses it and `bool(result)` is
   ALWAYS True; the `success` field even DEFAULTS to True.
   `_parse_chain_result` must read the verdict field, never truthiness.

2. The SDK's return value is the submitter reporting on itself. The only
   proof a commit exists is reading TimelockedWeightCommits back from
   the chain; `WeightSubmitter.submit` must verify, and a claim without
   storage behind it must come back False so the loop retries instead of
   marking the epoch done.
"""

import asyncio
import importlib.util
import sys
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

# The bittensor SDK is not installed in the test environment; stub just
# enough for `import bittensor as bt` and the bt.Keypair annotation.
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

from validator.chain import submitter as submitter_mod
from validator.chain.bittensor import BittensorChain
from validator.chain.submitter import WeightSubmitter


class FakeExtrinsicResponse:
    """Mirrors bittensor 10.5.0 core/types.py ExtrinsicResponse semantics:
    __iter__ / __getitem__ / __len__ (== 2), and — critically — NO
    __bool__, so truthiness falls through to __len__ and is always True."""

    def __init__(self, success=True, message=None, error=None):
        self.success = success
        self.message = message
        self.error = error

    def __iter__(self):
        yield self.success
        yield self.message

    def __getitem__(self, index):
        return (self.success, self.message)[index]

    def __len__(self):
        return 2


def _parse(result):
    # _parse_chain_result does not touch self; call it unbound.
    return BittensorChain._parse_chain_result(None, result, "set_weights")


class TestParseChainResult:
    def test_the_trap_itself_a_rejected_response_is_truthy(self):
        rejected = FakeExtrinsicResponse(success=False, message="rate limited")
        # This assertion documents the defect's mechanism: any code that
        # reads bool(result) calls a rejection a success.
        assert bool(rejected) is True
        assert _parse(rejected) is False

    def test_a_successful_response_parses_true(self):
        assert _parse(FakeExtrinsicResponse(success=True)) is True

    def test_the_default_constructed_response_is_not_trusted_blindly(self):
        # `success` defaults to True in the SDK; parsing must still read
        # the field (True here) rather than the object's truthiness —
        # asserted via the rejected case above; this pins the field read.
        resp = FakeExtrinsicResponse()
        resp.success = False
        assert _parse(resp) is False

    def test_legacy_tuple_shapes_still_work(self):
        assert _parse((False, "rejected")) is False
        assert _parse((True, "ok")) is True

    def test_plain_booleans_pass_through(self):
        assert _parse(True) is True
        assert _parse(False) is False

    def test_an_unrecognized_shape_is_a_failure_not_a_success(self):
        # Fail-closed: a response we cannot read must never count as
        # submitted — that is the exact defect this function had.
        assert _parse(object()) is False
        assert _parse("ok") is False


class _FakeChain:
    """ChainInterface test double for the submitter."""

    def __init__(self, claimed=True, proofs=None):
        self.claimed = claimed
        # proofs consumed one per verify call; last one repeats.
        self.proofs = list(proofs or [])
        self.verify_calls = 0
        self.set_weights_calls = 0

    async def get_current_block(self):
        return 8_888_000

    async def set_weights(self, uids, weights):
        self.set_weights_calls += 1
        return self.claimed

    async def verify_weight_commit_landed(self, since_block):
        self.verify_calls += 1
        if not self.proofs:
            return None
        if len(self.proofs) > 1:
            return self.proofs.pop(0)
        return self.proofs[0]


def _submitter(chain):
    weights_config = types.SimpleNamespace(burn_sink_uid=108)
    return WeightSubmitter(chain, weights_config)


@pytest.fixture(autouse=True)
def _fast_verification(monkeypatch):
    monkeypatch.setattr(submitter_mod, "_VERIFY_ATTEMPTS", 3)
    monkeypatch.setattr(submitter_mod, "_VERIFY_DELAY_SECS", 0.0)


LANDED = {
    "landed": True,
    "epoch": 24_605,
    "commit_block": 8_888_002,
    "reveal_round": 31_512_900,
    "ct_len": 399,
}
ABSENT = {"landed": False, "epochs_checked": [24_605, 24_606], "since_block": 8_888_000}


class TestSubmitterVerifiesOnChain:
    @pytest.mark.asyncio
    async def test_a_claimed_success_backed_by_storage_is_a_success(self):
        chain = _FakeChain(claimed=True, proofs=[LANDED])
        assert await _submitter(chain).submit([(1, 0.5)], 0.5) is True
        assert chain.verify_calls == 1

    @pytest.mark.asyncio
    async def test_a_claimed_success_with_no_commit_on_chain_is_a_failure(self):
        chain = _FakeChain(claimed=True, proofs=[ABSENT])
        assert await _submitter(chain).submit([(1, 0.5)], 0.5) is False

    @pytest.mark.asyncio
    async def test_an_unreadable_chain_is_a_failure_not_a_success(self):
        # None means "could not read" — absence of an answer is not an
        # answer, and fail-closed is safe: a duplicate retry is rejected
        # by the chain's own rate limit if the original landed.
        chain = _FakeChain(claimed=True, proofs=[None])
        assert await _submitter(chain).submit([(1, 0.5)], 0.5) is False
        assert chain.verify_calls == 3  # polled to exhaustion

    @pytest.mark.asyncio
    async def test_inclusion_lag_is_tolerated(self):
        chain = _FakeChain(claimed=True, proofs=[ABSENT, LANDED])
        assert await _submitter(chain).submit([(1, 0.5)], 0.5) is True
        assert chain.verify_calls == 2

    @pytest.mark.asyncio
    async def test_a_rejected_claim_fails_without_consulting_the_chain(self):
        chain = _FakeChain(claimed=False, proofs=[LANDED])
        assert await _submitter(chain).submit([(1, 0.5)], 0.5) is False
        assert chain.verify_calls == 0


class TestLoopLeavesFailedEpochsUnprocessed:
    """A failed submit must leave the epoch unprocessed and the tempo
    guard unarmed, so the 60s cycle retries until it lands or a newer
    epoch supersedes it. Marking the epoch anyway was the retry
    suppression: the false success became a permanently missed
    weight-set with no error anywhere."""

    def _loop(self, submit_result):
        from validator.weights.loop import WeightLoop

        loop = WeightLoop.__new__(WeightLoop)
        loop.chain = types.SimpleNamespace(
            get_current_block=AsyncMock(return_value=8_888_100)
        )
        loop.store = types.SimpleNamespace(mark_epoch_processed=AsyncMock())
        loop.submitter = types.SimpleNamespace(
            submit=AsyncMock(return_value=submit_result)
        )
        loop._cu_retry_start = {}
        loop._last_submitted_block = None
        return loop

    @pytest.mark.asyncio
    async def test_declared_burn_failure_is_not_marked_processed(self):
        loop = self._loop(submit_result=False)
        done = await loop._process_epoch("8888161", {})
        assert done is False
        loop.store.mark_epoch_processed.assert_not_awaited()
        assert loop._last_submitted_block is None

    @pytest.mark.asyncio
    async def test_declared_burn_success_is_marked_with_submitted_true(self):
        loop = self._loop(submit_result=True)
        done = await loop._process_epoch("8888161", {})
        assert done is True
        loop.store.mark_epoch_processed.assert_awaited_once_with(
            "8888161", weights_submitted=True
        )
        assert loop._last_submitted_block == 8_888_100

    @pytest.mark.asyncio
    async def test_cu_timeout_burn_failure_keeps_the_expired_timer(self):
        from validator.weights.loop import _CU_RETRY_TIMEOUT

        loop = self._loop(submit_result=False)
        # Non-empty configs + no manifest -> empty CU allocations path;
        # timer seeded already past the timeout.
        loop._cu_retry_start = {"8888161": time.time() - _CU_RETRY_TIMEOUT - 1}
        done = await loop._process_epoch("8888161", {"hk": {}}, manifest=None)
        assert done is False
        loop.store.mark_epoch_processed.assert_not_awaited()
        # The expired timer survives, so the next cycle submits
        # immediately instead of waiting another full timeout.
        assert "8888161" in loop._cu_retry_start
        assert loop._last_submitted_block is None

    @pytest.mark.asyncio
    async def test_save_audit_and_mark_skips_everything_on_failure(self):
        from validator.weights.loop import WeightLoop

        loop = WeightLoop.__new__(WeightLoop)
        loop.chain = types.SimpleNamespace(
            get_current_block=AsyncMock(return_value=8_888_100)
        )
        loop.store = types.SimpleNamespace(
            mark_epoch_processed=AsyncMock(), save_epoch_audit=AsyncMock()
        )
        loop._last_submitted_block = None
        await loop._save_audit_and_mark(
            "8888161", None, 2.0, 400.0, {}, {}, submitted=False
        )
        loop.store.mark_epoch_processed.assert_not_awaited()
        loop.store.save_epoch_audit.assert_not_awaited()
        assert loop._last_submitted_block is None
