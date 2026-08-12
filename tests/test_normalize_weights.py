"""normalize_weights: the u16 conversion that decides what actually
gets submitted on-chain.

These tests exist because the previous implementation pinned burn to
MAX_WEIGHT and scaled every miner as ``(w / burn_weight) * MAX_WEIGHT``.
Two legitimate states were unrepresentable: burn == 0 returned 100% to
the burn UID (paying no miner at all), and any burn below the largest
miner's share overflowed u16. Those cases are pinned here permanently.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

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

from validator.common.scoring.weights import normalize_weights
from validator.common.types import MAX_WEIGHT

BURN_UID = 108


def as_dict(result):
    d = dict(result)
    assert len(d) == len(result), "duplicate UIDs in result"
    return d


class TestZeroBurn:
    def test_zero_burn_pays_miners_not_burn(self):
        # The bug this file exists for: burn absent must mean miners
        # split everything, NOT "everything to burn".
        result = as_dict(normalize_weights([(1, 0.6), (2, 0.4)], BURN_UID))
        assert BURN_UID not in result
        assert result[1] == MAX_WEIGHT  # largest scales to max
        assert result[2] == pytest.approx(MAX_WEIGHT * 0.4 / 0.6, abs=1)

    def test_explicit_zero_burn_entry_is_dropped(self):
        result = as_dict(
            normalize_weights([(1, 0.6), (2, 0.4), (BURN_UID, 0.0)], BURN_UID)
        )
        assert BURN_UID not in result

    def test_near_zero_burn_does_not_overflow_u16(self):
        # Previously 1e-12 burn produced miner weights ~2.6e16.
        result = as_dict(
            normalize_weights([(1, 0.5), (BURN_UID, 1e-12)], BURN_UID)
        )
        assert all(0 < w <= MAX_WEIGHT for w in result.values())
        assert result[1] == MAX_WEIGHT

    def test_negative_float_residue_burn(self):
        # -1e-16 burn used to hit the <= 0 guard and burn everything.
        result = as_dict(
            normalize_weights([(1, 0.5), (BURN_UID, -1e-16)], BURN_UID)
        )
        assert result[1] == MAX_WEIGHT
        assert BURN_UID not in result


class TestBurnBelowLargestMiner:
    def test_burn_below_largest_miner_share_representable(self):
        # 26% burn with a 40% miner used to overflow; now the miner is
        # the reference and burn scales under it.
        result = as_dict(
            normalize_weights(
                [(1, 0.40), (2, 0.34), (BURN_UID, 0.26)], BURN_UID
            )
        )
        assert all(0 < w <= MAX_WEIGHT for w in result.values())
        assert result[1] == MAX_WEIGHT
        # proportions survive within quantisation
        assert result[BURN_UID] / result[1] == pytest.approx(0.26 / 0.40, rel=1e-3)
        assert result[2] / result[1] == pytest.approx(0.34 / 0.40, rel=1e-3)


class TestSteadyStateEquivalence:
    """When burn IS the largest weight — production today — the new
    max-normalization must reproduce the old burn-pinned behaviour."""

    def test_burn_largest_matches_old_algorithm(self):
        miners = [(1, 0.20), (2, 0.15), (3, 0.05)]
        burn = 0.60
        result = as_dict(normalize_weights(miners + [(BURN_UID, burn)], BURN_UID))
        assert result[BURN_UID] == MAX_WEIGHT
        for uid, w in miners:
            # old formula: (w / burn_weight) * MAX_WEIGHT
            assert result[uid] == pytest.approx((w / burn) * MAX_WEIGHT, abs=1)

    def test_live_epoch_shape(self):
        # A representative multi-miner distribution with burn largest.
        weights = [
            (1, 0.261), (2, 0.150), (3, 0.144), (4, 0.136),
            (5, 0.097), (6, 0.043), (7, 0.032), (8, 0.020),
            (BURN_UID, 0.382),
        ]
        result = as_dict(normalize_weights(weights, BURN_UID))
        assert result[BURN_UID] == MAX_WEIGHT
        assert all(0 < w <= MAX_WEIGHT for w in result.values())
        # ordering preserved
        vals = [result[u] for u in (1, 2, 3, 4, 5, 6, 7, 8)]
        assert vals == sorted(vals, reverse=True)


class TestDegenerateInputs:
    def test_empty_input_burns(self):
        # Nothing payable: fail closed to full burn, never submit empty.
        assert normalize_weights([], BURN_UID) == [(BURN_UID, MAX_WEIGHT)]

    def test_all_zero_weights_burns(self):
        assert normalize_weights([(1, 0.0), (2, 0.0)], BURN_UID) == [
            (BURN_UID, MAX_WEIGHT)
        ]

    def test_single_miner_takes_max(self):
        result = as_dict(normalize_weights([(7, 0.123)], BURN_UID))
        assert result == {7: MAX_WEIGHT}

    def test_burn_only_still_burns(self):
        result = as_dict(normalize_weights([(BURN_UID, 0.4)], BURN_UID))
        assert result == {BURN_UID: MAX_WEIGHT}

    def test_nan_and_negative_weights_dropped(self):
        result = as_dict(
            normalize_weights(
                [(1, 0.5), (2, float("nan")), (3, -0.2)], BURN_UID
            )
        )
        assert set(result) == {1}


class TestInvariants:
    @pytest.mark.parametrize("seed", range(20))
    def test_output_bounds_and_proportionality(self, seed):
        import random

        rng = random.Random(seed)
        n = rng.randint(1, 40)
        weights = [(uid, rng.random()) for uid in range(1, n + 1)]
        if rng.random() < 0.7:
            weights.append((BURN_UID, rng.random() * 2))
        result = normalize_weights(weights, BURN_UID)
        d = as_dict(result)
        # 1. every value in u16 range
        assert all(0 < w <= MAX_WEIGHT for w in d.values())
        # 2. the largest input is pinned to MAX_WEIGHT
        largest_uid = max(
            (x for x in weights if x[1] and x[1] > 0),
            key=lambda x: x[1],
            default=None,
        )
        if largest_uid is not None:
            assert d[largest_uid[0]] == MAX_WEIGHT
        # 3. pairwise proportions survive within quantisation error
        positive = [(u, w) for u, w in weights if w and w > 0]
        for (ua, wa), (ub, wb) in zip(positive, positive[1:]):
            if ua in d and ub in d and d[ub] >= 100:
                assert d[ua] / d[ub] == pytest.approx(wa / wb, rel=0.02)
