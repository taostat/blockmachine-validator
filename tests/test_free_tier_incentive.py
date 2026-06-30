"""Tests for free-tier incentive (T5): CU split, alpha computation, and the
per-coldkey report."""

from __future__ import annotations

import asyncio
import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest


# Stub heavy optional deps so importing WeightLoop (-> chain -> bittensor; ->
# api -> s3_repository -> botocore) works without the real packages. They are
# only used inside methods, not at import.
for _name in (
    "bittensor",
    "bittensor_drand",
    "bittensor_wallet",
    "async_substrate_interface",
):
    sys.modules.setdefault(_name, types.ModuleType(_name))

if "boto3" not in sys.modules:
    _boto3 = types.ModuleType("boto3")
    _boto3.client = MagicMock()
    sys.modules["boto3"] = _boto3
if "botocore" not in sys.modules:
    _botocore = types.ModuleType("botocore")
    _botocore.UNSIGNED = object()
    sys.modules["botocore"] = _botocore
if "botocore.config" not in sys.modules:
    _cfg = types.ModuleType("botocore.config")

    class _Config:
        def __init__(self, *args, **kwargs):
            pass

    _cfg.Config = _Config
    sys.modules["botocore.config"] = _cfg
if "botocore.exceptions" not in sys.modules:
    _exc = types.ModuleType("botocore.exceptions")

    # Must match the shape the other tests' stub expects (.response attr) so the
    # two stubs are interchangeable regardless of import order.
    class _ClientError(Exception):
        def __init__(self, error_response=None, operation_name=None):
            self.response = error_response
            self.operation_name = operation_name
            super().__init__(str(error_response))

    _exc.ClientError = _ClientError
    sys.modules["botocore.exceptions"] = _exc

from validator.common.scoring.weights import compute_epoch_weights  # noqa: E402
from validator.common.types import (  # noqa: E402
    EpochWeightsResult,
    MinerEpochData,
    MinerWeight,
)
from validator.api.logs_client import LogsClient  # noqa: E402
from validator.weights.loop import WeightLoop  # noqa: E402


# --------------------------------------------------------------------------- #
# CU split (schema v3) in _aggregate_cu_allocations
# --------------------------------------------------------------------------- #

def _agg(data: dict) -> dict:
    totals: dict[str, dict] = {}
    LogsClient._aggregate_cu_allocations(data, totals)
    return totals


def test_v3_splits_free_tier_cu():
    data = {
        "schema_version": 3,
        "allocations": [
            {
                "miner_hotkey": "hk1",
                "request_type_cus": [
                    {"method": "eth_call", "cu": 100, "archive": False, "free_tier": False},
                    {"method": "eth_call", "cu": 7, "archive": False, "free_tier": True},
                    {"method": "trace", "cu": 50, "archive": True, "free_tier": False},
                ],
            }
        ],
    }
    t = _agg(data)["hk1"]
    assert t["total"] == 157
    assert t["non_archive"] == 107
    assert t["archive"] == 50
    assert t["free_non_archive"] == 7
    assert t["free_archive"] == 0


def test_v2_has_zero_free_tier():
    data = {
        "schema_version": 2,
        "allocations": [
            {
                "miner_hotkey": "hk1",
                "request_type_cus": [{"method": "eth_call", "cu": 100, "archive": False}],
            }
        ],
    }
    t = _agg(data)["hk1"]
    assert t["non_archive"] == 100
    assert t["free_non_archive"] == 0
    assert t["free_archive"] == 0


# --------------------------------------------------------------------------- #
# Free-tier alpha in compute_epoch_weights
# --------------------------------------------------------------------------- #

def test_free_tier_alpha_is_share_of_payout():
    # Single miner, under-subscribed (scale=1): payout_alpha == consumed/price.
    miner = MinerEpochData(
        hotkey="hk1",
        coldkey="ck1",
        cu_total=100,
        target_usd_per_cu=0.01,
        cu_non_archive=100,
        price_non_archive=0.01,
        cu_free_tier_non_archive=10,  # 10% of CU is free-tier
    )
    res = compute_epoch_weights([miner], alpha_price_usd=2.0, emissions_alpha=1000.0)
    m = res.miners[0]
    # scale = 1 (pool >> consumed). consumed=1.0 USD, free=0.1 USD.
    assert abs(m.payout_alpha - (1.0 * res.scale) / 2.0) < 1e-9
    assert abs(m.free_tier_payout_alpha - (0.1 * res.scale) / 2.0) < 1e-9
    # free-tier alpha is exactly 10% of payout alpha (same scale/price).
    assert abs(m.free_tier_payout_alpha - 0.1 * m.payout_alpha) < 1e-9


def test_banned_miner_has_zero_free_tier_alpha():
    miner = MinerEpochData(
        hotkey="hk1",
        coldkey="ck1",
        cu_total=100,
        target_usd_per_cu=0.01,
        is_blacklisted=True,
        cu_non_archive=100,
        price_non_archive=0.01,
        cu_free_tier_non_archive=100,
    )
    res = compute_epoch_weights([miner], alpha_price_usd=2.0, emissions_alpha=1000.0)
    assert res.miners[0].free_tier_payout_alpha == 0.0


# --------------------------------------------------------------------------- #
# Per-coldkey report
# --------------------------------------------------------------------------- #

def _loop_with_reporter(reporter):
    return WeightLoop(
        config=MagicMock(),
        chain=MagicMock(),
        logs=MagicMock(),
        blacklist=MagicMock(),
        store=MagicMock(),
        prices=MagicMock(),
        submitter=MagicMock(),
        free_tier_reporter=reporter,
    )


def _weights_result(miners: list[MinerWeight]) -> EpochWeightsResult:
    return EpochWeightsResult(
        pool_usd_total=0,
        pool_total_cu=0,
        pool_usd_per_cu=0,
        total_consumed_usd=0,
        scale=1.0,
        burn_alpha=0,
        burn_pct=0,
        burn_weight=0,
        alpha_price_usd=1.0,
        emissions_alpha=0,
        miners=miners,
    )


def test_build_report_aggregates_by_coldkey_to_base_units():
    # two hotkeys share ck1; ck2 has a tiny amount; ck3 floors to 0 -> excluded.
    miners = [
        MinerWeight("hk1", 0, 0.0, False, free_tier_payout_alpha=1.0),
        MinerWeight("hk2", 0, 0.0, False, free_tier_payout_alpha=0.5),
        MinerWeight("hk3", 0, 0.0, False, free_tier_payout_alpha=0.000000002),
        MinerWeight("hk4", 0, 0.0, False, free_tier_payout_alpha=0.0),
    ]
    coldkey_map = {"hk1": "ck1", "hk2": "ck1", "hk3": "ck2", "hk4": "ck3"}

    per_coldkey = WeightLoop._build_free_tier_report(
        _weights_result(miners), coldkey_map
    )
    by = {d["coldkey"]: d["incentive_alpha"] for d in per_coldkey}
    # ck1 = (1.0 + 0.5) alpha * 1e9 = 1_500_000_000 base units
    assert by["ck1"] == "1500000000"
    # ck2 = 0.000000002 alpha * 1e9 = 2 base units
    assert by["ck2"] == "2"
    # ck3 floors to 0 -> not reported
    assert "ck3" not in by


def test_build_report_empty_for_burn_epoch():
    assert WeightLoop._build_free_tier_report(None, None) == []


@pytest.mark.asyncio
async def test_report_dispatches_to_reporter():
    reporter = AsyncMock()
    loop = _loop_with_reporter(reporter)
    miners = [MinerWeight("hk1", 0, 0.0, False, free_tier_payout_alpha=1.0)]

    await loop._report_free_tier("epoch1", _weights_result(miners), {"hk1": "ck1"})
    # fire-and-forget: drain the dispatched background task, then assert.
    await asyncio.gather(*loop._free_tier_tasks)

    reporter.report_free_tier_incentive.assert_awaited_once_with(
        "epoch1", [{"coldkey": "ck1", "incentive_alpha": "1000000000"}]
    )


@pytest.mark.asyncio
async def test_report_empty_for_burn_dispatches():
    reporter = AsyncMock()
    loop = _loop_with_reporter(reporter)
    await loop._report_free_tier("epoch_burn")
    await asyncio.gather(*loop._free_tier_tasks)
    reporter.report_free_tier_incentive.assert_awaited_once_with("epoch_burn", [])


@pytest.mark.asyncio
async def test_no_reporter_is_noop():
    loop = _loop_with_reporter(None)
    # Must not raise or dispatch when reporter is absent.
    await loop._report_free_tier("epoch1")
    assert not loop._free_tier_tasks
