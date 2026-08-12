"""compute_epoch_weights scale semantics: unclamped pool fill.

The scale was min(1.0, pool/consumed): under-consumption pinned scale at
1.0 and burned the remainder. It is now pool/consumed in both directions,
bounded by SCALE_SANITY_MAX — under-consumption scales payouts UP until
the pool is exactly filled and burn is zero, while a collapse of
consumption (upstream allocation never injected) falls back to the old
clamped behaviour rather than handing the whole pool to whoever had
traffic. These tests pin all of that.
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

from validator.common.scoring.weights import (
    MINER_EMISSION_PCT,
    SCALE_SANITY_MAX,
    compute_epoch_weights,
)
from validator.common.types import MinerEpochData

ALPHA_PRICE = 2.0
EMISSIONS = 360.0  # tempo
POOL_ALPHA = EMISSIONS * MINER_EMISSION_PCT
POOL_USD = POOL_ALPHA * ALPHA_PRICE


def miner(hotkey: str, cu: float, price: float) -> MinerEpochData:
    return MinerEpochData(
        hotkey=hotkey,
        coldkey="ck-" + hotkey,
        cu_total=cu,
        cu_archive=0.0,
        cu_non_archive=0.0,
        price_archive=0.0,
        price_non_archive=0.0,
        target_usd_per_cu=price,
        is_blacklisted=False,
    )


def run(miners):
    return compute_epoch_weights(miners, ALPHA_PRICE, EMISSIONS)


class TestUnderConsumptionFillsPool:
    def test_burn_zero_when_consumed_below_pool(self):
        # consumed = 62% of pool — previously 38% burned; now zero.
        r = run([miner("a", 100.0, POOL_USD * 0.004),  # $0.4/cu * 100 = 40% pool
                 miner("b", 100.0, POOL_USD * 0.0022)])  # 22% pool
        assert r.scale == pytest.approx(1 / 0.62, rel=1e-9)
        assert r.burn_alpha == pytest.approx(0.0, abs=1e-9)
        assert r.burn_pct == pytest.approx(0.0, abs=1e-12)

    def test_pool_exactly_distributed(self):
        r = run([miner("a", 50.0, 1.0), miner("b", 150.0, 1.0)])
        total_alpha = sum(m.payout_alpha for m in r.miners)
        assert total_alpha == pytest.approx(POOL_ALPHA, rel=1e-12)

    def test_split_is_pure_consumption_share(self):
        r = run([miner("a", 30.0, 1.0), miner("b", 10.0, 1.0)])
        a, b = r.miners
        assert a.weight == pytest.approx(0.75, rel=1e-12)
        assert b.weight == pytest.approx(0.25, rel=1e-12)

    def test_price_cancels_out_of_weights(self):
        # Holds only while scale stays inside the sanity band — outside it
        # the fail-closed fallback deliberately reintroduces the price
        # dependence (see TestSanityBound). Both prices here keep
        # pool/consumed under SCALE_SANITY_MAX.
        m = [miner("a", 30.0, 1.0), miner("b", 10.0, 1.0)]
        w_low = [x.weight for x in compute_epoch_weights(m, 0.5, EMISSIONS).miners]
        w_high = [x.weight for x in compute_epoch_weights(m, 2.5, EMISSIONS).miners]
        assert w_low == pytest.approx(w_high, rel=1e-12)
        assert w_low == pytest.approx([0.75, 0.25], rel=1e-12)


class TestOverConsumptionUnchanged:
    def test_scale_below_one_still_caps_at_pool(self):
        # consumed = 2x pool: same as the old behaviour.
        r = run([miner("a", 100.0, POOL_USD * 0.02)])
        assert r.scale == pytest.approx(0.5, rel=1e-12)
        assert r.burn_alpha == pytest.approx(0.0, abs=1e-9)
        total_alpha = sum(m.payout_alpha for m in r.miners)
        assert total_alpha == pytest.approx(POOL_ALPHA, rel=1e-12)


class TestSanityBound:
    def test_collapse_falls_back_to_clamped_burn(self):
        # consumed is 1/60th of the pool — the outage/capture shape. Must
        # NOT hand the pool over; must pay face value and burn the rest.
        consumed = POOL_USD / 60.0
        r = run([miner("a", 1.0, consumed)])
        assert r.scale == 1.0
        assert r.miners[0].payout_usd == pytest.approx(consumed, rel=1e-12)
        assert r.burn_alpha == pytest.approx(
            POOL_ALPHA - consumed / ALPHA_PRICE, rel=1e-9
        )
        assert r.burn_pct > 0.9

    def test_just_inside_bound_still_fills_pool(self):
        consumed = POOL_USD / (SCALE_SANITY_MAX - 0.5)
        r = run([miner("a", 1.0, consumed)])
        assert r.scale == pytest.approx(SCALE_SANITY_MAX - 0.5, rel=1e-9)
        assert r.burn_alpha == pytest.approx(0.0, abs=1e-9)

    def test_just_past_bound_burns(self):
        consumed = POOL_USD / (SCALE_SANITY_MAX + 0.5)
        r = run([miner("a", 1.0, consumed)])
        assert r.scale == 1.0
        assert r.burn_pct > 0.0


class TestDegenerate:
    def test_zero_consumption_full_burn(self):
        r = run([miner("a", 0.0, 1.0)])
        assert r.scale == 1.0
        assert r.burn_alpha == pytest.approx(POOL_ALPHA, rel=1e-12)
        assert r.burn_weight == pytest.approx(1.0, rel=1e-12)

    def test_no_miners_full_burn(self):
        r = run([])
        assert r.burn_weight == pytest.approx(1.0, rel=1e-12)

    def test_blacklisted_excluded_from_split(self):
        m = [miner("a", 100.0, 1.0), miner("b", 100.0, 1.0)]
        m[1].is_blacklisted = True
        r = run(m)
        assert r.miners[1].consumed_usd == 0.0
        assert r.miners[1].weight == 0.0
        assert r.miners[0].weight == pytest.approx(1.0, rel=1e-12)

    def test_burn_never_negative(self):
        # Float summation dust is clamped: burn is exactly >= 0 always.
        r = run([miner("a", 7.0, 3.1415), miner("b", 11.0, 2.7182)])
        assert r.burn_alpha >= 0.0
        assert r.burn_weight >= 0.0
