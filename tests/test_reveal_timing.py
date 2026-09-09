"""Reveal timing: the commit has to be decryptable when its epoch runs.

THE DEFECT. The weight loop's "already submitted" guard computed its window
as ``block // tempo``, a lattice anchored at block 0. The chain's epoch
boundary is a stateful counter that has drifted off that lattice — measured
3,329 blocks (~11h) apart on mainnet 2026-09-09. So a validator's commit time
was decided by where its lattice line happened to fall, not by the epoch, and
once out of phase it stayed out because the guard repeated the same cadence
forever. ``blocks_until_next_epoch`` had the same arithmetic, so the
"is there time to retry" check was wrong by the same margin.

THE SECOND HALF. The SDK chooses its drand round by predicting the boundary
as ``blocks_remaining * block_time`` and then adding a 3-block safety offset,
which puts the target PAST the boundary: measured +2.8 blocks (~36s, ~12 drand
rounds) at EVERY commit position, so it is not a phase effect and no commit
position escapes it on its own. Real blocks average a little over 12s and that
drift is the only thing that ever rescues the commit — which is why at the two
epochs in the observed six where the chain ran at exactly nominal speed, the
entire validator set missed and scored the previous epoch's weights.

A missed reveal is not lost — the pallet retries it every block and it lands
at the following boundary. The damage is that the cohort SPLITS: validators
whose pulse arrived disagree with those whose did not, and validator_trust
punishes the disagreement. At mainnet epoch 24624 that cost four validators
12% of trust while two scored 1.0000.

Run: python3 -m pytest tests/test_reveal_timing.py
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

from validator.chain.submitter import WeightSubmitter
from validator.config import ValidatorConfig
from validator.weights.loop import WeightLoop, _SDK_REVEAL_OFFSET_BLOCKS

# Mainnet, 2026-09-09. The epoch that split the cohort.
TEMPO = 7200
LAST_EPOCH_BLOCK = 9017729
BOUNDARY = LAST_EPOCH_BLOCK + TEMPO  # 9024929
BLOCK_TIME_FLOOR = 12.000
# The slowest 7200-block window in 30 days of mainnet; the fastest was exactly
# the floor, 12.000000, integer-exact, and nothing was ever below it.
BLOCK_TIME_OBSERVED_MAX = 12.038333
DEFAULT_MARGIN = 45.0


class FakeChain:
    def __init__(self, block, last_epoch_block=LAST_EPOCH_BLOCK, tempo=TEMPO):
        self.block = block
        self.last_epoch_block = last_epoch_block
        self.tempo = tempo
        self.anchor_reads = 0
        self.anchor_fails = False
        self.set_weights_calls = []

    async def get_current_block(self):
        return self.block

    async def get_tempo(self):
        return self.tempo

    async def get_last_epoch_block(self):
        self.anchor_reads += 1
        if self.anchor_fails:
            return None
        return self.last_epoch_block

    async def set_weights(self, uids, weights, block_time=None):
        self.set_weights_calls.append(block_time)
        return True

    async def verify_weight_commit_landed(self, since_block):
        return {"landed": True, "epoch": 1, "commit_block": self.block}

    def get_validator_hotkey(self):
        return "5Fake"


def loop_with(chain, **weight_overrides):
    config = ValidatorConfig()
    for k, v in weight_overrides.items():
        setattr(config.weights, k, v)
    return WeightLoop(
        config=config,
        chain=chain,
        logs=AsyncMock(),
        blacklist=AsyncMock(),
        store=AsyncMock(),
        prices=AsyncMock(),
        submitter=WeightSubmitter(chain, config.weights),
    )


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. the guard is keyed to the chain's epoch, not to a lattice
# ---------------------------------------------------------------------------


def test_the_guard_uses_the_chain_boundary_not_block_over_tempo():
    """THE BUG, PINNED — with the exact numbers that produced it on mainnet.

    The lattice line at 9,021,600 falls INSIDE the chain epoch that runs
    9,017,729 -> 9,024,929. uid 67 committed at 9,017,770, right after the
    real boundary; when the lattice line passed at 9,021,600 the old guard
    saw a "new window" and released it to commit a SECOND time in the same
    chain epoch, six blocks later. That second commit is the one that missed
    its reveal.

    The two clocks must disagree here or the test proves nothing: the lattice
    says "new window, go ahead", the chain says "you already committed this
    epoch". An earlier version of this test picked a block where both agreed
    and passed against the bug — caught by mutation, not by review.
    """
    lattice_line = 9021600
    assert LAST_EPOCH_BLOCK < lattice_line < BOUNDARY, (
        "fixture must straddle a lattice line inside one chain epoch"
    )
    first_commit = LAST_EPOCH_BLOCK + 41  # uid 103/202's position
    now = lattice_line + 6  # uid 67's second commit

    # The old arithmetic would allow it...
    assert first_commit // TEMPO != now // TEMPO, (
        "fixture no longer reproduces the lattice releasing a second commit"
    )
    # ...the chain says it is the same epoch.
    assert first_commit >= LAST_EPOCH_BLOCK

    chain = FakeChain(block=now)
    loop = loop_with(chain, commit_window_blocks=0)
    loop._last_submitted_block = first_commit
    assert run(loop._ready_to_commit()) is False, (
        "committed twice in one chain epoch — the guard is reading the lattice"
    )


def test_a_commit_from_the_previous_epoch_does_not_block_this_one():
    chain = FakeChain(block=BOUNDARY - 50)
    loop = loop_with(chain)
    loop._last_submitted_block = LAST_EPOCH_BLOCK - 5
    assert run(loop._ready_to_commit()) is True


def test_a_fresh_validator_commits():
    chain = FakeChain(block=BOUNDARY - 50)
    loop = loop_with(chain)
    assert loop._last_submitted_block is None
    assert run(loop._ready_to_commit()) is True


# ---------------------------------------------------------------------------
# 2. the commit window
# ---------------------------------------------------------------------------


def test_the_commit_is_held_until_the_boundary_is_close():
    """Freshness is unaffected — every commit inside an epoch drains at the
    same boundary — so holding costs nothing and shrinks the window in which
    the ciphertext is publicly decryptable."""
    chain = FakeChain(block=LAST_EPOCH_BLOCK + 40)  # 7,160 blocks to go
    loop = loop_with(chain, commit_window_blocks=200)
    assert run(loop._ready_to_commit()) is False
    chain.block = BOUNDARY - 200
    assert run(loop._ready_to_commit()) is True


def test_a_window_of_zero_disables_holding():
    """An operator who wants the old commit-immediately behaviour can have it
    from config, without a release."""
    chain = FakeChain(block=LAST_EPOCH_BLOCK + 40)
    loop = loop_with(chain, commit_window_blocks=0)
    assert run(loop._ready_to_commit()) is True


def test_a_late_commit_is_still_sent_because_skipping_costs_the_epoch():
    """THE BUG THIS ALMOST SHIPPED WITH.

    The first version refused to commit with too little runway, reasoning
    that the commit could not be decrypted in time and the previous vector
    stayed in `Weights` anyway. That reasoning ignores the activity cutoff.

    `LastUpdate` is written at COMMIT time and the reveal never touches it,
    so on a commit-reveal subnet it IS the last commit block. The epoch marks
    a validator inactive when `last_update + activity_cutoff < current_block`,
    and the cutoff is one whole tempo. Committing once per epoch keeps
    consecutive commits one tempo apart, comfortably inside it. Skip one and
    the gap doubles: the validator is masked out of `active_stake` and earns
    NOTHING for that epoch.

    A late commit costs one stale epoch. A skipped commit costs the epoch's
    entire income. Never skip.
    """
    for remaining in (5, 1, 19):
        chain = FakeChain(block=BOUNDARY - remaining)
        loop = loop_with(chain, commit_min_runway_blocks=20)
        assert run(loop._ready_to_commit()) is True, (
            f"refused to commit with {remaining} blocks left — that skips an "
            "epoch and zeroes this validator's dividends"
        )


def test_the_normal_cadence_stays_inside_the_activity_cutoff():
    """One commit per epoch at a fixed offset puts consecutive commits exactly
    one tempo apart, and the cutoff is one tempo — so the margin is the offset
    itself. Committing 200 blocks before the boundary leaves 7,000 blocks of
    margin; committing right after the boundary would leave 32."""
    cutoff = TEMPO  # ActivityCutoffFactorMilli(1000) * tempo / 1000
    for window in (200, 600):
        commit_at = BOUNDARY - window
        age_at_next_boundary = (BOUNDARY + TEMPO) - (commit_at + TEMPO)
        assert age_at_next_boundary == window
        assert commit_at + cutoff >= BOUNDARY, (
            "this cadence would be judged inactive at its own boundary"
        )
        assert cutoff - age_at_next_boundary >= TEMPO - 600


# ---------------------------------------------------------------------------
# 3. the reveal round actually lands before the boundary
# ---------------------------------------------------------------------------


def aimed_seconds(block_time, blocks_to_boundary):
    """What the SDK will aim at, given the block_time we pass it."""
    return (blocks_to_boundary + _SDK_REVEAL_OFFSET_BLOCKS) * block_time


def test_the_target_lands_before_the_earliest_possible_boundary():
    """THE ARITHMETIC THAT DECIDES EVERYTHING. The boundary cannot arrive
    sooner than blocks * 12.000s, so aiming below that is aiming before it —
    whatever the chain does afterwards. Checked at both ends of the window and
    at the position where the old fixed-constant approach failed."""
    for remaining in (200, 100, 20, 600, 7195):
        chain = FakeChain(block=BOUNDARY - remaining)
        loop = loop_with(chain, reveal_margin_secs=15.0)
        bt = run(loop._reveal_block_time())
        assert bt is not None, remaining
        earliest = remaining * BLOCK_TIME_FLOOR
        aimed = aimed_seconds(bt, remaining)
        slack = earliest - aimed
        assert abs(slack - 15.0) < 1e-6, (
            f"{remaining} blocks out: aimed {aimed:.1f}s against an earliest "
            f"boundary of {earliest:.1f}s — slack {slack:.1f}s, wanted 15s"
        )


def test_the_sdk_default_is_guaranteed_to_miss_at_the_floor():
    """Not "usually misses" — at nominal block speed the default cannot land.
    This is the control: it shows the fix is not fixing a coin flip."""
    for remaining in (200, 3323, 7160):
        aimed = aimed_seconds(12.0, remaining)
        earliest = remaining * BLOCK_TIME_FLOOR
        assert aimed > earliest, remaining
        assert abs((aimed - earliest) - 36.0) < 1e-6, (
            "the SDK's overshoot should be a constant 36s (3 blocks) at every "
            f"position; at {remaining} blocks it was {aimed - earliest:.1f}s"
        )


def test_exposure_shrinks_with_the_horizon():
    """Why the commit window exists. The ciphertext is decryptable early by
    the margin PLUS the drift accumulated over the horizon, so a short horizon
    is the only lever on it — and the fix must not be sold as removing it."""
    def exposure(remaining):
        chain = FakeChain(block=BOUNDARY - remaining)
        loop = loop_with(chain, reveal_margin_secs=15.0)
        bt = run(loop._reveal_block_time())
        return remaining * BLOCK_TIME_OBSERVED_MAX - aimed_seconds(bt, remaining)

    near, far = exposure(200), exposure(7195)
    # Measured at the slowest observed block time: ~26s at 200 blocks out
    # against ~291s over a full epoch. The property is the ratio — the
    # horizon is the only lever — not either figure on its own.
    assert near < 40, f"200 blocks out should expose <40s, got {near:.0f}s"
    assert far > 200, f"a full-epoch horizon should expose >200s, got {far:.0f}s"
    assert far / near > 5, f"exposure barely moved with the horizon: {near:.0f}s vs {far:.0f}s"


def test_the_margin_is_configurable_without_a_release():
    """Ben's condition: operators upgrade once. Tightening the exposure later
    must be a config change, not another version."""
    chain = FakeChain(block=BOUNDARY - 200)
    tight = run(loop_with(chain, reveal_margin_secs=5.0)._reveal_block_time())
    loose = run(loop_with(chain, reveal_margin_secs=60.0)._reveal_block_time())
    assert tight > loose, "a larger margin must aim earlier"
    assert abs((200 * BLOCK_TIME_FLOOR - aimed_seconds(loose, 200)) - 60.0) < 1e-6


def test_the_block_time_floor_is_the_load_bearing_constant():
    """It is a measured lower bound (30 days, 88 windows, minimum exactly
    12.000000). Raising it above the truth silently breaks the guarantee, so
    the default is pinned here."""
    assert ValidatorConfig().weights.block_time_floor_secs == 12.0


# ---------------------------------------------------------------------------
# 4. the six observed mainnet epochs
# ---------------------------------------------------------------------------

# Measured block time over each commit window, against whether the median
# committer's reveal landed at the boundary that consumed it. Monotonic: the
# epochs everyone missed are exactly the epochs the chain ran near nominal.
OBSERVED = (
    (12.0000000, False),
    (12.0016667, False),
    (12.0200000, True),
    (12.0400000, True),
    (12.0250000, True),
    (12.0083333, True),  # the knife edge — landed by zero rounds
)


def test_the_fix_lands_at_every_one_of_the_six_observed_epochs():
    """The SDK default made four of six. The point of the fix is that the two
    it missed were not bad luck: they were the epochs at nominal speed, where
    the default cannot land at all. Aiming at the floor removes the
    dependence on drift entirely, so all six land — including the two that
    were previously impossible."""
    remaining = 200
    chain = FakeChain(block=BOUNDARY - remaining)
    loop = loop_with(chain, reveal_margin_secs=15.0)
    bt = run(loop._reveal_block_time())
    aimed = aimed_seconds(bt, remaining)

    default_landed = 0
    fixed_landed = 0
    for real_block_time, _sdk_outcome in OBSERVED:
        real_seconds_to_boundary = remaining * real_block_time
        if aimed_seconds(12.0, remaining) <= real_seconds_to_boundary:
            default_landed += 1
        if aimed <= real_seconds_to_boundary:
            fixed_landed += 1
    assert fixed_landed == len(OBSERVED), (
        f"the fix landed {fixed_landed}/{len(OBSERVED)} observed epochs"
    )
    assert default_landed < len(OBSERVED), (
        "the SDK default landed every observed epoch — this fixture no longer "
        "reproduces the defect"
    )


# ---------------------------------------------------------------------------
# 5. an unreadable chain must not make things worse
# ---------------------------------------------------------------------------


def test_an_unreadable_anchor_falls_back_to_a_rolling_cadence():
    """Conservative direction: it can delay a commit, never duplicate one.
    The old lattice is not reinstated, because its phase lock is the bug."""
    chain = FakeChain(block=BOUNDARY - 50)
    chain.anchor_fails = True
    loop = loop_with(chain)
    loop._last_submitted_block = chain.block - 100
    assert run(loop._ready_to_commit()) is False, "re-committed 100 blocks later"
    loop._last_submitted_block = chain.block - (TEMPO + 1)
    assert run(loop._ready_to_commit()) is True


def test_an_unreadable_anchor_leaves_the_sdk_default_rather_than_guessing():
    chain = FakeChain(block=BOUNDARY - 200)
    chain.anchor_fails = True
    loop = loop_with(chain)
    assert run(loop._reveal_block_time()) is None


def test_a_first_commit_is_never_blocked_by_an_unreadable_anchor():
    chain = FakeChain(block=BOUNDARY - 50)
    chain.anchor_fails = True
    loop = loop_with(chain)
    assert run(loop._ready_to_commit()) is True


def test_the_boundary_having_passed_is_not_a_negative_block_time():
    """`blocks_to_boundary` can be <= 0 between the boundary firing and the
    anchor advancing. That must not produce a nonsense target."""
    chain = FakeChain(block=BOUNDARY + 3)
    loop = loop_with(chain)
    assert run(loop._reveal_block_time()) is None


def test_a_margin_larger_than_the_runway_does_not_invert_the_target():
    """A misconfigured margin must degrade to the SDK default, not aim at a
    negative time."""
    chain = FakeChain(block=BOUNDARY - 5)
    loop = loop_with(chain, reveal_margin_secs=600.0, commit_min_runway_blocks=1)
    assert run(loop._reveal_block_time()) is None


# ---------------------------------------------------------------------------
# 6. the runway check the retry loop depends on
# ---------------------------------------------------------------------------


def test_blocks_until_next_epoch_uses_the_chain_boundary():
    """This value decides whether a failed submit is retried. On the lattice
    it could report 200 blocks left with 3,500 to go, abandoning submissions
    that had hours in hand."""
    chain = FakeChain(block=BOUNDARY - 300)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.blocks_until_next_epoch()) == 300
    lattice_answer = TEMPO - (chain.block % TEMPO)
    assert lattice_answer != 300, "fixture no longer distinguishes the two clocks"


def test_blocks_until_next_epoch_falls_back_and_never_goes_negative():
    chain = FakeChain(block=BOUNDARY + 100)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.blocks_until_next_epoch()) == 0
    chain.anchor_fails = True
    assert run(submitter.blocks_until_next_epoch()) == TEMPO - (chain.block % TEMPO)


# ---------------------------------------------------------------------------
# 7. the value actually reaches the chain
# ---------------------------------------------------------------------------


def test_the_block_time_is_passed_through_to_set_weights():
    """A computed round nobody sends is not a fix."""
    chain = FakeChain(block=BOUNDARY - 200)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    ok = run(submitter.submit([(1, 0.5)], 0.5, block_time=11.98))
    assert ok is True
    assert chain.set_weights_calls == [11.98], chain.set_weights_calls


def test_omitting_the_block_time_leaves_the_sdk_default():
    """And it must not be passed as an explicit None either: a chain
    implementation predating the keyword has to keep working, which is how
    this change broke eight existing tests before it was made conditional."""
    chain = FakeChain(block=BOUNDARY - 200)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    run(submitter.submit([(1, 0.5)], 0.5))
    assert chain.set_weights_calls == [None]


def test_a_chain_that_predates_the_keyword_still_commits():
    class OldChain(FakeChain):
        async def set_weights(self, uids, weights):  # no block_time
            self.set_weights_calls.append("called")
            return True

    chain = OldChain(block=BOUNDARY - 200)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.submit([(1, 0.5)], 0.5)) is True
    assert chain.set_weights_calls == ["called"]


def test_both_submit_paths_compute_a_reveal_round():
    """The retry path and the burn-only direct path both commit weights, so a
    fix applied to one of them is a fix that fires half the time."""
    chain = FakeChain(block=BOUNDARY - 200)
    loop = loop_with(chain)
    run(loop._submit_direct([(1, 0.5)], 0.5))
    assert chain.set_weights_calls and chain.set_weights_calls[-1] is not None
    chain.set_weights_calls.clear()
    run(loop._submit_with_retry([(1, 0.5)], 0.5))
    assert chain.set_weights_calls and chain.set_weights_calls[-1] is not None

# ---------------------------------------------------------------------------
# 8. the SDK offset is measured, not trusted
# ---------------------------------------------------------------------------


def _fake_drand(offset, constant_round=False):
    """A stand-in for bittensor_drand whose offset we choose."""
    mod = types.ModuleType("bittensor_drand")

    def get_encrypted_commit_v2(**kw):
        if constant_round:
            return b"", 1_000_000
        remaining = kw["tempo"] - kw["blocks_since_last_step"]
        seconds = (remaining + offset) * kw["block_time"]
        return b"", int(1_000_000 + seconds / 3.0)

    mod.get_encrypted_commit_v2 = get_encrypted_commit_v2
    return mod


def _with_drand(mod):
    saved = sys.modules.get("bittensor_drand")
    if mod is None:
        sys.modules.pop("bittensor_drand", None)
    else:
        sys.modules["bittensor_drand"] = mod
    return saved


def test_the_offset_is_derived_from_the_installed_library():
    """The margin degrades ~12s per unit of offset error: at 4 it is +3s, at
    5 it is -9s and the fix is gone — silently, on a dependency bump nobody
    audited. So the value is measured, not compiled in."""
    from validator.weights.loop import measure_reveal_offset_blocks

    saved = _with_drand(_fake_drand(offset=7))
    try:
        assert measure_reveal_offset_blocks() == 7
    finally:
        _with_drand(saved)


def test_an_implausible_measurement_falls_back_to_the_constant():
    """A margin computed from nonsense is worse than the known constant."""
    from validator.weights.loop import (
        measure_reveal_offset_blocks,
        _SDK_REVEAL_OFFSET_BLOCKS,
    )

    saved = _with_drand(_fake_drand(offset=0, constant_round=True))
    try:
        assert measure_reveal_offset_blocks() == _SDK_REVEAL_OFFSET_BLOCKS
    finally:
        _with_drand(saved)


def test_a_missing_library_falls_back_to_the_constant():
    from validator.weights.loop import (
        measure_reveal_offset_blocks,
        _SDK_REVEAL_OFFSET_BLOCKS,
    )

    saved = _with_drand(None)
    try:
        assert measure_reveal_offset_blocks() == _SDK_REVEAL_OFFSET_BLOCKS
    finally:
        _with_drand(saved)


def test_the_measured_offset_is_the_one_the_solve_uses():
    """Measuring it and then not using it would be the same bug with extra
    steps."""
    remaining = 200
    chain = FakeChain(block=BOUNDARY - remaining)
    loop = loop_with(chain, reveal_margin_secs=15.0)
    loop._reveal_offset_blocks = 9
    bt = run(loop._reveal_block_time())
    aimed = 200 * BLOCK_TIME_FLOOR - 15.0
    assert abs(bt - aimed / (remaining + 9)) < 1e-9, (
        "the solve is not dividing by the measured offset"
    )


def test_the_probe_schedule_is_self_consistent():
    """The library reads both `blocks_since_last_step` and
    `current_block - last_epoch_block`. Feeding it two different answers to
    the same question derived an offset of 1004 blocks on the first attempt."""
    seen = {}

    mod = types.ModuleType("bittensor_drand")

    def get_encrypted_commit_v2(**kw):
        seen.update(kw)
        remaining = kw["tempo"] - kw["blocks_since_last_step"]
        return b"", int(1_000_000 + (remaining + 3) * kw["block_time"] / 3.0)

    mod.get_encrypted_commit_v2 = get_encrypted_commit_v2
    from validator.weights.loop import measure_reveal_offset_blocks

    saved = _with_drand(mod)
    try:
        measure_reveal_offset_blocks()
    finally:
        _with_drand(saved)
    assert seen["current_block"] - seen["last_epoch_block"] == seen[
        "blocks_since_last_step"
    ], "the probe told the library two different things about its own schedule"

# ---------------------------------------------------------------------------
# 9. the two P1s codex found against this diff
# ---------------------------------------------------------------------------


def test_a_commit_confirmed_after_the_boundary_does_not_suppress_the_next_one():
    """CODEX P1, AND IT IS THE SAME MONEY BUG THROUGH A DIFFERENT DOOR.

    `submit` polls storage for up to a minute to prove the commit landed, so
    a commit INCLUDED at B-1 can be CONFIRMED at B+1. Recording the
    confirmation block made the per-epoch guard read "already committed this
    epoch" for the whole of the new epoch — no commit went out, and at the
    following boundary the real LastUpdate was over the activity cutoff and
    dividends were zero.
    """
    class SubmitterWithProof:
        last_commit_block = BOUNDARY - 1  # included just before the boundary

    chain = FakeChain(block=BOUNDARY + 100, last_epoch_block=BOUNDARY)
    loop = loop_with(chain, commit_window_blocks=0)
    loop.submitter = SubmitterWithProof()
    loop._last_submitted_block = run(loop._commit_block())
    assert loop._last_submitted_block == BOUNDARY - 1, (
        "recorded the confirmation block instead of the inclusion block"
    )
    assert run(loop._ready_to_commit()) is True, (
        "a commit from the previous epoch suppressed this epoch's commit — "
        "that is a full epoch of dividends"
    )


def test_the_commit_block_falls_back_when_no_proof_carries_one():
    class NoProof:
        last_commit_block = None

    chain = FakeChain(block=BOUNDARY - 10)
    loop = loop_with(chain)
    loop.submitter = NoProof()
    assert run(loop._commit_block()) == BOUNDARY - 10


def test_the_submitter_records_the_verified_inclusion_block():
    chain = FakeChain(block=BOUNDARY - 5)

    class ProvingChain(FakeChain):
        async def verify_weight_commit_landed(self, since_block):
            return {"landed": True, "commit_block": BOUNDARY - 6}

    chain = ProvingChain(block=BOUNDARY - 5)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.submit([(1, 0.5)], 0.5)) is True
    assert submitter.last_commit_block == BOUNDARY - 6


def test_a_verified_commit_with_no_block_in_the_proof_uses_the_presubmit_block():
    """Never later than inclusion. Recording a later block suppresses the
    next epoch's commit; an earlier one only risks a duplicate the chain's
    own rate limit rejects."""
    class VagueChain(FakeChain):
        async def verify_weight_commit_landed(self, since_block):
            return {"landed": True}

    chain = VagueChain(block=BOUNDARY - 5)
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.submit([(1, 0.5)], 0.5)) is True
    assert submitter.last_commit_block == BOUNDARY - 5


def test_the_margin_covers_a_whole_block_of_intra_block_elapsed_time():
    """CODEX P1. The library adds its predicted duration to wall-clock NOW,
    but predicts in whole blocks — so being partway through the current block
    silently eats up to 12s of margin. And the pulse must reach
    `Drand::LastStoredRound` before the reveal runs, which wants it published
    a block ahead rather than merely before the boundary block. 15s covered
    neither and could land inside one drand round of the boundary."""
    assert ValidatorConfig().weights.reveal_margin_secs >= 36.0, (
        "the margin no longer covers one block of intra-block drift plus a "
        "block of ingestion lead"
    )
    for remaining in (200, 20):
        chain = FakeChain(block=BOUNDARY - remaining)
        loop = loop_with(chain)
        bt = run(loop._reveal_block_time())
        aimed = aimed_seconds(bt, remaining)
        # Worst case: we sampled the block 11.9s ago, so the boundary is that
        # much nearer in wall-clock than the block count implies.
        lead = (remaining * BLOCK_TIME_FLOOR - 11.9) - aimed
        assert lead >= 12.0, (
            f"{remaining} blocks out: only {lead:.1f}s of lead in the worst "
            "intra-block case — under one block, so the pulse may not be "
            "ingested before the reveal runs"
        )


def test_exposure_is_still_under_a_minute_at_the_commit_window():
    """The larger margin is bought with exposure, and Ben accepted ~1 minute.
    This pins that the trade did not quietly get worse."""
    remaining = 200
    chain = FakeChain(block=BOUNDARY - remaining)
    loop = loop_with(chain)
    bt = run(loop._reveal_block_time())
    exposure = remaining * BLOCK_TIME_OBSERVED_MAX - aimed_seconds(bt, remaining)
    assert exposure < 60.0, f"exposure grew to {exposure:.0f}s"
