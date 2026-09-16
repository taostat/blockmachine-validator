"""Commit-reveal OFF: set the weights the moment the epoch is final, and
prove the set from `Weights`, not from a commit queue that no longer exists.

WHY. On a commit-reveal subnet the vector reaches `Weights` only when the
drand round the ciphertext was locked to is published and the chain applies
the reveal, and that has to happen before the epoch boundary block. Every
validator here computes the same vector from the same published payload, so
the timelock protects nothing, and its timing is the one way a correct
vector can miss its epoch: on 2026-09-15 the reveal landed one block after
the boundary and the whole subnet was paid by the previous day's vector.
With `CommitRevealWeightsEnabled` off, a plain set_weights is in `Weights`
the block it is included and pays out at the next boundary.

WHAT CHANGES, AND WHAT MUST NOT.
  * The flag is READ from the chain, every time; it is a hyperparameter the
    subnet owner flips at a block of his choosing, and the validator must
    follow it in both directions with no release in between.
  * A flag that cannot be read means ON. The commit path is the one every
    deployed validator has run for months; the plain-set path is taken only
    on a definite "off". Taking it by accident would skip the commit window
    and prove the submit against storage the commit never writes.
  * OFF: no 200-block hold and no runway floor — the vector goes on chain
    as soon as the finalized epoch exists. The once-per-chain-epoch rule
    stays: `LastUpdate` has to keep advancing inside the activity cutoff,
    and the chain rejects a second set inside `WeightsSetRateLimit`.
  * OFF: the proof is `Weights[netuid, uid]` equal to the submitted vector,
    u16 for u16 and order-independent, AND `LastUpdate[uid]` at or after
    the pre-submit block. The second half is what makes it a proof of THIS
    submit: a re-send of the vector already on chain matches `Weights`
    before it is even sent. Both are read from storage, never from the
    cached metagraph, which can be two minutes stale.
  * OFF: `block_time` is not passed (there is no round to aim at) and the
    reveal-target log line is not written.
  * ON: nothing changes. Every pre-existing test in this suite runs with a
    chain double that has no flag at all, and passes unchanged.

Run: python3 -m pytest tests/test_commit_reveal_off.py
"""

import asyncio
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

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
from validator.config import ValidatorConfig
from validator.weights.loop import WeightLoop

# Mainnet, 2026-09-16 (read from storage while this was written).
TEMPO = 7200
LAST_EPOCH_BLOCK = 9_082_529
BOUNDARY = LAST_EPOCH_BLOCK + TEMPO
OUR_UID = 103
HOTKEY = "5FakeValidatorHotkeyForTheProofReadBack000000000"


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@pytest.fixture(autouse=True)
def _fast_verification(monkeypatch):
    monkeypatch.setattr(submitter_mod, "_VERIFY_ATTEMPTS", 3)
    monkeypatch.setattr(submitter_mod, "_VERIFY_DELAY_SECS", 0.0)


# --------------------------------------------------------------------------
# The chain double. `flag` is what CommitRevealWeightsEnabled reads as:
# True / False / None (unreadable) / "raise" / "absent" (a chain that
# predates the read).
# --------------------------------------------------------------------------
class FlaglessChain:
    """A chain that predates the flag read, or a test double without it."""

    def __init__(self, block=BOUNDARY - 7000, weights_proofs=None, commit_proofs=None):
        self.block = block
        self.set_weights_calls = []
        self.weights_proofs = list(weights_proofs or [])
        self.commit_proofs = list(commit_proofs or [])
        self.weights_verify_calls = 0
        self.commit_verify_calls = 0
        self.pending_commit_reads = 0

    async def get_current_block(self):
        return self.block

    async def get_tempo(self):
        return TEMPO

    async def get_last_epoch_block(self):
        return LAST_EPOCH_BLOCK

    async def set_weights(self, uids, weights, **kwargs):
        self.set_weights_calls.append((list(uids), list(weights), dict(kwargs)))
        return True

    async def verify_weights_set(self, since_block, uids, weights):
        self.weights_verify_calls += 1
        if not self.weights_proofs:
            return None
        if len(self.weights_proofs) > 1:
            return self.weights_proofs.pop(0)
        return self.weights_proofs[0]

    async def verify_weight_commit_landed(self, since_block):
        self.commit_verify_calls += 1
        if not self.commit_proofs:
            return None
        if len(self.commit_proofs) > 1:
            return self.commit_proofs.pop(0)
        return self.commit_proofs[0]

    async def get_pending_weight_commits(self):
        self.pending_commit_reads += 1
        return []

    async def get_blocks_since_last_update(self):
        return 10

    def get_validator_hotkey(self):
        return HOTKEY


class FakeChain(FlaglessChain):
    def __init__(self, flag, **kw):
        super().__init__(**kw)
        self.flag = flag

    async def commit_reveal_enabled(self):
        if self.flag == "raise":
            raise RuntimeError("websocket closed")
        return self.flag


def chain_with(flag, **kw):
    """`flag`: True / False / None (unreadable) / "raise" / "absent"."""
    if flag == "absent":
        return FlaglessChain(**kw)
    return FakeChain(flag, **kw)


def submitter_with(chain):
    return WeightSubmitter(chain, types.SimpleNamespace(burn_sink_uid=108))


def loop_with(chain, submitter=None):
    config = ValidatorConfig()
    return WeightLoop(
        config=config,
        chain=chain,
        logs=AsyncMock(),
        blacklist=AsyncMock(),
        store=AsyncMock(),
        prices=AsyncMock(),
        submitter=submitter or WeightSubmitter(chain, config.weights),
    )


SET_LANDED = {"landed": True, "set_block": BOUNDARY - 6999, "uid": OUR_UID, "n": 2}
SET_ABSENT = {"landed": False, "since_block": BOUNDARY - 7000, "last_update": 1,
              "matches": False}
COMMIT_LANDED = {"landed": True, "epoch": 1, "commit_block": BOUNDARY - 6999,
                 "reveal_round": 1, "ct_len": 1}


# ==========================================================================
# The submitter picks its proof by the flag.
# ==========================================================================
class TestSubmitterFollowsTheFlag:
    def test_off_proves_the_set_from_weights_and_passes_no_block_time(self):
        chain = FakeChain(False, weights_proofs=[SET_LANDED])
        sub = submitter_with(chain)
        assert run(sub.submit([(1, 0.5)], 0.5, block_time=11.9)) is True
        assert chain.weights_verify_calls == 1
        assert chain.commit_verify_calls == 0
        (uids, values, kwargs), = chain.set_weights_calls
        assert "block_time" not in kwargs, "no round to aim at when the flag is off"
        # The dedupe block is the block the SET was included at, from the
        # proof — not the block verification finished at.
        assert sub.last_commit_block == SET_LANDED["set_block"]

    def test_off_a_set_the_chain_does_not_show_is_not_submitted(self):
        chain = FakeChain(False, weights_proofs=[SET_ABSENT])
        sub = submitter_with(chain)
        assert run(sub.submit([(1, 0.5)], 0.5)) is False
        assert chain.weights_verify_calls == 3  # polled to exhaustion
        assert sub.last_commit_block is None

    def test_off_an_unreadable_chain_is_not_submitted(self):
        chain = FakeChain(False, weights_proofs=[None])
        assert run(submitter_with(chain).submit([(1, 0.5)], 0.5)) is False

    def test_off_inclusion_lag_is_tolerated(self):
        chain = FakeChain(False, weights_proofs=[SET_ABSENT, SET_LANDED])
        assert run(submitter_with(chain).submit([(1, 0.5)], 0.5)) is True
        assert chain.weights_verify_calls == 2

    def test_off_a_raising_verifier_is_could_not_read_not_a_crash(self):
        chain = FakeChain(False)

        async def boom(since_block, uids, weights):
            raise RuntimeError("verifier bug")

        chain.verify_weights_set = boom
        assert run(submitter_with(chain).submit([(1, 0.5)], 0.5)) is False

    def test_on_is_unchanged_commit_proof_and_block_time(self):
        chain = FakeChain(True, commit_proofs=[COMMIT_LANDED])
        sub = submitter_with(chain)
        assert run(sub.submit([(1, 0.5)], 0.5, block_time=11.9)) is True
        assert chain.commit_verify_calls == 1
        assert chain.weights_verify_calls == 0
        (_, _, kwargs), = chain.set_weights_calls
        assert kwargs == {"block_time": 11.9}
        assert sub.last_commit_block == COMMIT_LANDED["commit_block"]

    @pytest.mark.parametrize("flag", [None, "raise", "absent"])
    def test_an_unreadable_flag_means_on(self, flag):
        """Never the new path by accident."""
        chain = chain_with(flag, commit_proofs=[COMMIT_LANDED])
        sub = submitter_with(chain)
        assert run(sub.submit([(1, 0.5)], 0.5, block_time=11.9)) is True
        assert chain.commit_verify_calls == 1
        assert chain.weights_verify_calls == 0
        (_, _, kwargs), = chain.set_weights_calls
        assert kwargs == {"block_time": 11.9}

    def test_the_vector_proved_is_the_normalized_u16_vector_that_was_sent(self):
        """The read-back must compare against exactly what went to the
        chain, not the float input: the SDK's own conversion is an identity
        on a max-normalized u16 vector, so what we send is what storage
        holds."""
        seen = {}

        class RecordingChain(FakeChain):
            async def verify_weights_set(self, since_block, uids, weights):
                seen["uids"], seen["weights"] = list(uids), list(weights)
                return SET_LANDED

        chain = RecordingChain(False)
        assert run(submitter_with(chain).submit([(1, 0.25), (2, 0.5)], 0.0)) is True
        (uids, values, _), = chain.set_weights_calls
        assert (seen["uids"], seen["weights"]) == (uids, values)
        assert max(values) == 65535 and all(isinstance(v, int) for v in values)


# ==========================================================================
# The adapter's read-back proof, against a fake substrate. Every read is a
# storage query; the fake has no metagraph at all, so a proof that used
# the metagraph cache could not pass.
# ==========================================================================
class FakeSubstrate:
    def __init__(self, uid=OUR_UID, weights=None, last_update=None, fail=None):
        self.uid = uid
        self.weights = weights
        self.last_update = last_update
        self.fail = fail
        self.queries = []

    def query(self, module, item, params):
        self.queries.append((module, item, params))
        if self.fail == item:
            raise RuntimeError(f"{item} read failed")
        if item == "Uids":
            return types.SimpleNamespace(value=self.uid)
        if item == "Weights":
            return types.SimpleNamespace(value=self.weights)
        if item == "LastUpdate":
            return types.SimpleNamespace(value=self.last_update)
        raise AssertionError(f"unexpected storage read {item}")


def adapter_with(substrate):
    chain = BittensorChain.__new__(BittensorChain)
    chain.netuid = 19
    chain.hotkey = types.SimpleNamespace(ss58_address=HOTKEY)
    chain.subtensor = types.SimpleNamespace(substrate=substrate)
    chain._subtensor_lock = asyncio.Lock()
    chain._consecutive_failures = 0
    return chain


def last_update_vec(ours, n=256):
    vec = [0] * n
    vec[OUR_UID] = ours
    return vec


SINCE = 9_082_600
SENT_UIDS = [206, 108, 84]
SENT_VALUES = [65535, 4333, 43]


class TestWeightsReadBack:
    def test_the_set_is_proved_from_weights_and_last_update_in_storage(self):
        sub = FakeSubstrate(
            weights=[(84, 43), (108, 4333), (206, 65535)],  # storage order: by uid
            last_update=last_update_vec(SINCE + 1),
        )
        proof = run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))
        assert proof == {"landed": True, "set_block": SINCE + 1, "uid": OUR_UID, "n": 3}
        assert [q[1] for q in sub.queries] == ["Uids", "Weights", "LastUpdate"]

    def test_order_does_not_matter_but_every_entry_does(self):
        sub = FakeSubstrate(
            weights=[(84, 43), (108, 4334), (206, 65535)],  # one u16 off
            last_update=last_update_vec(SINCE + 1),
        )
        proof = run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))
        assert proof["landed"] is False
        assert proof["matches"] is False

    def test_a_missing_or_extra_entry_is_not_our_vector(self):
        for on_chain in (
            [(84, 43), (206, 65535)],
            [(84, 43), (108, 4333), (206, 65535), (7, 1)],
        ):
            sub = FakeSubstrate(weights=on_chain, last_update=last_update_vec(SINCE + 1))
            proof = run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))
            assert proof["landed"] is False, on_chain

    def test_the_same_vector_from_before_the_submit_is_not_proof(self):
        """A re-send of the vector already on chain matches `Weights`
        before the extrinsic is even sent. Only `LastUpdate` moving says
        the chain accepted THIS set."""
        sub = FakeSubstrate(
            weights=[(84, 43), (108, 4333), (206, 65535)],
            last_update=last_update_vec(SINCE - 1),
        )
        proof = run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))
        assert proof == {
            "landed": False, "since_block": SINCE, "last_update": SINCE - 1,
            "matches": True,
        }

    def test_last_update_at_exactly_the_presubmit_block_counts(self):
        # since_block is sampled before the submit; inclusion in that very
        # block is the earliest possible and must not read as "before".
        sub = FakeSubstrate(
            weights=[(84, 43), (108, 4333), (206, 65535)],
            last_update=last_update_vec(SINCE),
        )
        assert run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))["landed"]

    def test_an_empty_weights_row_is_not_landed(self):
        sub = FakeSubstrate(weights=None, last_update=last_update_vec(SINCE + 1))
        proof = run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))
        assert proof["landed"] is False

    def test_a_last_update_row_too_short_for_our_uid_is_not_landed(self):
        sub = FakeSubstrate(
            weights=[(84, 43), (108, 4333), (206, 65535)],
            last_update=[0] * OUR_UID,
        )
        proof = run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES))
        assert proof == {
            "landed": False, "since_block": SINCE, "last_update": None, "matches": True,
        }

    @pytest.mark.parametrize("failing", ["Uids", "Weights", "LastUpdate"])
    def test_any_read_failure_is_none_never_a_verdict(self, failing):
        sub = FakeSubstrate(
            weights=[(84, 43), (108, 4333), (206, 65535)],
            last_update=last_update_vec(SINCE + 1),
            fail=failing,
        )
        chain = adapter_with(sub)
        assert run(chain.verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES)) is None
        assert chain._consecutive_failures == 1

    def test_an_unregistered_hotkey_is_none(self):
        sub = FakeSubstrate(uid=None)
        assert run(adapter_with(sub).verify_weights_set(SINCE, SENT_UIDS, SENT_VALUES)) is None


class TestFlagRead:
    def test_the_flag_comes_from_the_sdk_read_of_the_hyperparameter(self):
        chain = adapter_with(FakeSubstrate())
        chain.subtensor = types.SimpleNamespace(
            commit_reveal_enabled=lambda netuid: {19: False}[netuid]
        )
        assert run(chain.commit_reveal_enabled()) is False
        chain.subtensor = types.SimpleNamespace(commit_reveal_enabled=lambda netuid: True)
        assert run(chain.commit_reveal_enabled()) is True

    def test_a_failed_flag_read_is_none(self):
        chain = adapter_with(FakeSubstrate())

        def boom(netuid):
            raise RuntimeError("no connection")

        chain.subtensor = types.SimpleNamespace(commit_reveal_enabled=boom)
        assert run(chain.commit_reveal_enabled()) is None
        assert chain._consecutive_failures == 1


# ==========================================================================
# The loop: no hold when off, the dedupe stays, the flag is followed.
# ==========================================================================
class TestLoopFollowsTheFlag:
    def test_off_commits_with_the_whole_epoch_ahead(self):
        # 7000 blocks out: the commit window (200) would hold this.
        loop = loop_with(FakeChain(False, block=BOUNDARY - 7000))
        assert run(loop._ready_to_commit()) is True

    def test_off_still_only_once_per_chain_epoch(self):
        loop = loop_with(FakeChain(False, block=BOUNDARY - 7000))
        loop._last_submitted_block = LAST_EPOCH_BLOCK + 1
        assert run(loop._ready_to_commit()) is False
        # The previous epoch's submit does not block this one.
        loop._last_submitted_block = LAST_EPOCH_BLOCK - 1
        assert run(loop._ready_to_commit()) is True

    def test_on_holds_for_the_window(self):
        loop = loop_with(FakeChain(True, block=BOUNDARY - 7000))
        assert run(loop._ready_to_commit()) is False
        loop = loop_with(FakeChain(True, block=BOUNDARY - 150))
        assert run(loop._ready_to_commit()) is True

    @pytest.mark.parametrize("flag", [None, "raise", "absent"])
    def test_an_unreadable_flag_holds_like_on(self, flag):
        loop = loop_with(chain_with(flag, block=BOUNDARY - 7000))
        assert run(loop._ready_to_commit()) is False

    def test_off_has_no_reveal_target(self):
        loop = loop_with(FakeChain(False, block=BOUNDARY - 150))
        assert run(loop._reveal_block_time()) is None

    def test_on_has_a_reveal_target(self):
        loop = loop_with(FakeChain(True, block=BOUNDARY - 150))
        assert run(loop._reveal_block_time()) is not None

    def test_off_the_dedupe_block_is_the_landed_set_block(self):
        """Through the real submitter: a declared-burn epoch is set, proved
        from `Weights`, and `_last_submitted_block` is the block the set
        was included at, so the once-per-epoch rule and the activity cutoff
        keep working exactly as on the commit path."""
        chain = FakeChain(False, block=BOUNDARY - 7000, weights_proofs=[SET_LANDED])
        loop = loop_with(chain)
        assert run(loop._process_epoch("9082561", {})) is True
        assert loop._last_submitted_block == SET_LANDED["set_block"]
        loop.store.mark_epoch_processed.assert_awaited_once_with(
            "9082561", weights_submitted=True
        )
        (uids, values, kwargs), = chain.set_weights_calls
        burn = ValidatorConfig().weights.burn_sink_uid
        assert (uids, values, kwargs) == ([burn], [65535], {})
        # And the next tick of the same epoch does not submit again.
        assert run(loop._ready_to_commit()) is False

    def test_off_a_failed_set_leaves_the_epoch_unprocessed(self):
        chain = FakeChain(False, block=BOUNDARY - 7000, weights_proofs=[SET_ABSENT])
        loop = loop_with(chain)
        assert run(loop._process_epoch("9082561", {})) is False
        assert loop._last_submitted_block is None
        loop.store.mark_epoch_processed.assert_not_awaited()

    def test_off_skips_the_commit_queue_tracking(self):
        chain = FakeChain(False, block=BOUNDARY - 7000)
        loop = loop_with(chain)
        loop.logs.fetch_finalized_epochs = AsyncMock(return_value=[])
        run(loop._tick())
        assert chain.pending_commit_reads == 0

    def test_on_tracks_the_commit_queue(self):
        chain = FakeChain(True, block=BOUNDARY - 7000)
        loop = loop_with(chain)
        loop.logs.fetch_finalized_epochs = AsyncMock(return_value=[])
        run(loop._tick())
        assert chain.pending_commit_reads == 1

    def test_the_flag_is_read_every_time_not_cached(self):
        """The owner flips it at a block of his choosing; the validator
        must follow within a tick, in both directions."""
        chain = FakeChain(True, block=BOUNDARY - 7000)
        loop = loop_with(chain)
        assert run(loop._ready_to_commit()) is False
        chain.flag = False
        assert run(loop._ready_to_commit()) is True
        chain.flag = True
        assert run(loop._ready_to_commit()) is False
