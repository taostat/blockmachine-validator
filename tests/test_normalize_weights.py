"""Tests for normalize_weights — verifies the largest-remainder
normalization produces correct on-chain proportions."""

import math
import sys
import types
from unittest.mock import Mock

import pytest

# Stub boto3/botocore before importing validator modules
if "boto3" not in sys.modules:
    boto3 = types.ModuleType("boto3")
    boto3.client = Mock()
    sys.modules["boto3"] = boto3
if "botocore" not in sys.modules:
    sys.modules["botocore"] = types.ModuleType("botocore")
if "botocore.config" not in sys.modules:
    config_mod = types.ModuleType("botocore.config")
    config_mod.Config = type("Config", (), {"__init__": lambda self, *a, **k: None})
    sys.modules["botocore.config"] = config_mod
if "botocore.exceptions" not in sys.modules:
    exc_mod = types.ModuleType("botocore.exceptions")
    exc_mod.ClientError = type("ClientError", (Exception,), {})
    sys.modules["botocore.exceptions"] = exc_mod

from validator.common.scoring.weights import normalize_weights  # noqa: E402
from validator.common.types import MAX_WEIGHT  # noqa: E402

BURN_UID = 103


def chain_normalize(weights: list[tuple[int, int]]) -> dict[int, int]:
    """Simulate on-chain max-normalization: scale so max value = MAX_WEIGHT."""
    if not weights:
        return {}
    max_w = max(w for _, w in weights)
    if max_w <= 0:
        return {uid: w for uid, w in weights}
    scale = MAX_WEIGHT / max_w
    return {uid: round(w * scale) for uid, w in weights}


def incentive_share(uid: int, on_chain: dict[int, int]) -> float:
    """Compute the on-chain incentive share for a UID after normalization."""
    total = sum(on_chain.values())
    return on_chain.get(uid, 0) / total if total else 0.0


# ── Real epoch data from mainnet logs ────────────────────────────────────────

# Epoch 8029723 — real float weights from the validator log
REAL_MINERS = {
    107: 0.00404153,
    138: 0.00304660,
    226: 0.00068025,
    5: 0.00034874,
    158: 0.00022634,
    140: 0.00021623,
    224: 0.00017878,
    51: 0.00003634,
    68: 0.00002418,
    179: 0.00002340,
}
REAL_BURN_WEIGHT = 1.0 - sum(REAL_MINERS.values())


def _build_input(miners: dict[int, float], burn_w: float) -> list[tuple[int, float]]:
    return [(uid, w) for uid, w in miners.items()] + [(BURN_UID, burn_w)]


class TestBasicContract:
    """The function must always return a valid weight vector."""

    def test_empty_input_returns_full_burn(self):
        result = normalize_weights([], BURN_UID)
        assert result == [(BURN_UID, MAX_WEIGHT)]

    def test_zero_weights_returns_full_burn(self):
        result = normalize_weights([(1, 0.0), (2, 0.0)], BURN_UID)
        assert result == [(BURN_UID, MAX_WEIGHT)]

    def test_only_burn_returns_full_burn(self):
        result = normalize_weights([(BURN_UID, 0.99)], BURN_UID)
        assert result == [(BURN_UID, MAX_WEIGHT)]

    def test_burn_always_gets_max_weight(self):
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        burn_vals = [w for uid, w in result if uid == BURN_UID]
        assert len(burn_vals) == 1
        assert burn_vals[0] == MAX_WEIGHT

    def test_all_weights_positive(self):
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        for uid, w in result:
            assert w > 0, f"UID {uid} has non-positive weight {w}"

    def test_all_weights_are_integers(self):
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        for uid, w in result:
            assert isinstance(w, int), f"UID {uid} weight is {type(w)}, not int"

    def test_no_duplicate_uids(self):
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        uids = [uid for uid, _ in result]
        assert len(uids) == len(set(uids)), "Duplicate UIDs in output"

    def test_negative_weights_filtered(self):
        inp = [(1, 0.001), (2, -0.5), (BURN_UID, 0.999)]
        result = normalize_weights(inp, BURN_UID)
        uids = {uid for uid, _ in result}
        assert 2 not in uids

    def test_miners_preserved(self):
        """All miners with weight > 0 should appear in output."""
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        out_uids = {uid for uid, _ in result if uid != BURN_UID}
        expected_uids = set(REAL_MINERS.keys())
        assert out_uids == expected_uids


class TestChainNormIsNoop:
    """Burn = MAX_WEIGHT means the chain's max-normalization changes nothing."""

    def test_chain_normalization_is_noop(self):
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        on_chain = chain_normalize(result)

        for uid, w in result:
            assert on_chain[uid] == w, (
                f"UID {uid}: submitted {w}, on-chain {on_chain[uid]} — "
                f"chain normalization changed the weight!"
            )


class TestProportionAccuracy:
    """The on-chain incentive shares must closely match intended weights."""

    def test_aggregate_miner_share_error_under_1pct(self):
        """Total miner share error must be <1% of intended total."""
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        on_chain = chain_normalize(result)
        total = sum(on_chain.values())

        intended_miner_share = sum(REAL_MINERS.values())
        actual_miner_share = sum(
            on_chain.get(uid, 0) for uid in REAL_MINERS
        ) / total

        relative_error = abs(actual_miner_share - intended_miner_share) / intended_miner_share
        assert relative_error < 0.01, (
            f"Aggregate miner share error {relative_error:.4%} exceeds 1%: "
            f"intended={intended_miner_share:.8f}, actual={actual_miner_share:.8f}"
        )

    def test_large_miner_error_under_1pct(self):
        """Miners with u16 weight >= 10 should have <1% relative error."""
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        on_chain = chain_normalize(result)
        total = sum(on_chain.values())

        for uid, intended in REAL_MINERS.items():
            u16 = on_chain.get(uid, 0)
            if u16 < 10:
                continue  # tiny miners have inherent quantization error
            actual = u16 / total
            rel_err = abs(actual - intended) / intended
            assert rel_err < 0.01, (
                f"UID {uid} (u16={u16}): relative error {rel_err:.4%} exceeds 1%"
            )

    def test_better_than_old_sum_to_max_method(self):
        """New method must produce lower aggregate error than old sum-to-65535."""
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)

        # New method
        new_result = normalize_weights(inp, BURN_UID)
        new_chain = chain_normalize(new_result)
        new_total = sum(new_chain.values())
        new_error = sum(
            abs(new_chain.get(uid, 0) / new_total - w)
            for uid, w in REAL_MINERS.items()
        )

        # Simulate old method: int(w * MAX_WEIGHT), burn = remainder
        old_result = {}
        running = 0
        for uid, w in REAL_MINERS.items():
            u16 = int(w * MAX_WEIGHT)
            if u16 > 0:
                old_result[uid] = u16
                running += u16
        old_result[BURN_UID] = MAX_WEIGHT - running
        # chain normalize old
        max_w = max(old_result.values())
        scale = MAX_WEIGHT / max_w
        old_chain = {u: round(w * scale) for u, w in old_result.items()}
        old_total = sum(old_chain.values())
        old_error = sum(
            abs(old_chain.get(uid, 0) / old_total - w)
            for uid, w in REAL_MINERS.items()
        )

        assert new_error < old_error, (
            f"New method error ({new_error:.10f}) should be less than "
            f"old method error ({old_error:.10f})"
        )


class TestEdgeCases:
    """Edge cases that could trip up the algorithm."""

    def test_single_miner(self):
        inp = [(42, 0.001), (BURN_UID, 0.999)]
        result = normalize_weights(inp, BURN_UID)
        uids = {uid for uid, _ in result}
        assert BURN_UID in uids
        assert 42 in uids
        burn_w = next(w for uid, w in result if uid == BURN_UID)
        assert burn_w == MAX_WEIGHT

    def test_very_small_miner_gets_at_least_1(self):
        """A miner whose ideal u16 is between 0.5 and 1.0 should get 1."""
        # weight/burn * 65535 ≈ 0.7 → should round up to 1 via LR
        inp = [(42, 0.00001), (BURN_UID, 0.99999)]
        result = normalize_weights(inp, BURN_UID)
        miner_w = [w for uid, w in result if uid == 42]
        assert len(miner_w) == 1
        assert miner_w[0] >= 1

    def test_very_tiny_miner_dropped(self):
        """A miner whose ideal u16 rounds to 0 should be dropped."""
        # weight/burn * 65535 ≈ 0.0007 → floors to 0, no LR extra
        inp = [(42, 0.00000001), (BURN_UID, 0.99999999)]
        result = normalize_weights(inp, BURN_UID)
        uids = {uid for uid, _ in result}
        assert 42 not in uids

    def test_many_equal_miners(self):
        """50 miners with equal weight — all should get the same u16."""
        w = 0.001
        miners = {i: w for i in range(50)}
        burn_w = 1.0 - sum(miners.values())
        inp = _build_input(miners, burn_w)
        result = normalize_weights(inp, BURN_UID)
        miner_weights = [mw for uid, mw in result if uid != BURN_UID]
        # All should be within 1 of each other (due to LR extras)
        assert max(miner_weights) - min(miner_weights) <= 1

    def test_high_burn_low_miners(self):
        """99.99% burn — extreme case similar to real SN19."""
        inp = [(1, 0.00005), (2, 0.00003), (3, 0.00002), (BURN_UID, 0.9999)]
        result = normalize_weights(inp, BURN_UID)
        burn_w = next(w for uid, w in result if uid == BURN_UID)
        assert burn_w == MAX_WEIGHT

    def test_no_burn_in_input(self):
        """If burn UID not in input, return full burn."""
        result = normalize_weights([(1, 0.5), (2, 0.5)], BURN_UID)
        assert result == [(BURN_UID, MAX_WEIGHT)]

    def test_result_order_has_burn_first(self):
        """Burn UID should be the first element (for log readability)."""
        inp = _build_input(REAL_MINERS, REAL_BURN_WEIGHT)
        result = normalize_weights(inp, BURN_UID)
        assert result[0][0] == BURN_UID
