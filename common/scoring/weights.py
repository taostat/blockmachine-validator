import logging
import math

from validator.common.types import (
    EpochWeightsResult,
    MinerEpochData,
    MinerWeight,
    MAX_WEIGHT,
)

logger = logging.getLogger(__name__)

MINER_EMISSION_PCT = 0.41


def compute_epoch_weights(
    miners_data: list[MinerEpochData],
    alpha_price_usd: float,
    emissions_alpha: float,
) -> EpochWeightsResult:
    if alpha_price_usd <= 0:
        raise ValueError("alpha_price_usd must be positive")
    if emissions_alpha <= 0:
        raise ValueError("emissions_alpha must be positive")

    miner_pool_alpha = emissions_alpha * MINER_EMISSION_PCT
    miner_pool_usd = miner_pool_alpha * alpha_price_usd
    pool_total_cu = sum(m.cu_total for m in miners_data)

    results: list[MinerWeight] = []
    total_consumed_usd = 0.0

    for miner in miners_data:
        if miner.is_blacklisted:
            consumed_usd = 0.0
        elif miner.cu_archive > 0 or miner.cu_non_archive > 0:
            consumed_usd = (
                miner.cu_non_archive * miner.price_non_archive
                + miner.cu_archive * miner.price_archive
            )
        else:
            consumed_usd = miner.cu_total * miner.target_usd_per_cu

        total_consumed_usd += consumed_usd
        results.append(
            MinerWeight(
                miner_hotkey=miner.hotkey,
                cu_total=miner.cu_total,
                target_usd_per_cu=miner.target_usd_per_cu,
                is_banned=miner.is_blacklisted,
                consumed_usd=consumed_usd,
            )
        )

    scale = (
        min(1.0, miner_pool_usd / total_consumed_usd) if total_consumed_usd > 0 else 1.0
    )

    total_payout_alpha = 0.0
    for r in results:
        r.payout_usd = r.consumed_usd * scale
        r.payout_alpha = r.payout_usd / alpha_price_usd
        total_payout_alpha += r.payout_alpha

    burn_alpha = miner_pool_alpha - total_payout_alpha
    burn_pct = burn_alpha / miner_pool_alpha if miner_pool_alpha > 0 else 0

    for r in results:
        r.weight = r.payout_alpha / miner_pool_alpha if miner_pool_alpha > 0 else 0

    burn_weight = burn_alpha / miner_pool_alpha if miner_pool_alpha > 0 else 0
    pool_usd_per_cu = total_consumed_usd / pool_total_cu if pool_total_cu > 0 else 0

    return EpochWeightsResult(
        pool_usd_total=miner_pool_usd,
        pool_total_cu=pool_total_cu,
        pool_usd_per_cu=pool_usd_per_cu,
        total_consumed_usd=total_consumed_usd,
        scale=scale,
        burn_alpha=burn_alpha,
        burn_pct=burn_pct,
        burn_weight=burn_weight,
        alpha_price_usd=alpha_price_usd,
        emissions_alpha=miner_pool_alpha,
        miners=results,
    )


def normalize_weights(
    miner_weights: list[tuple[int, float]], burn_uid: int
) -> list[tuple[int, int]]:
    """Normalize float weights to u16 values for on-chain submission.

    Burn UID is always set to MAX_WEIGHT (65535) so the chain's
    max-normalization (which scales the largest weight to 65535) is a
    no-op and the submitted proportions are preserved exactly.

    Miner weights use the largest-remainder method (the same algorithm
    used in proportional-representation elections) to optimally round
    fractional u16 values, minimising quantisation error to <1%.
    """
    cleaned: list[tuple[int, float]] = [
        (uid, w) for uid, w in miner_weights if w and w > 0
    ]

    if not cleaned:
        return [(burn_uid, MAX_WEIGHT)]

    # Extract the burn weight from the list
    burn_weight = 0.0
    miners_only: list[tuple[int, float]] = []
    for uid, w in cleaned:
        if uid == burn_uid:
            burn_weight = w
        else:
            miners_only.append((uid, w))

    if burn_weight <= 0 or not miners_only:
        return [(burn_uid, MAX_WEIGHT)]

    # Burn always gets MAX_WEIGHT.  Miner weights are scaled relative to
    # burn so the ratio miner/burn is preserved after the chain's
    # max-normalization pass.
    #
    # Largest-remainder rounding:
    #   1. Compute the ideal (fractional) u16 value for each miner.
    #   2. Give each miner the floor of their ideal value.
    #   3. Distribute the leftover points (ideal_total - floor_total)
    #      to the miners with the largest fractional remainders.
    ideals = [
        (uid, (w / burn_weight) * MAX_WEIGHT) for uid, w in miners_only
    ]

    floors = [
        (uid, math.floor(ideal), ideal - math.floor(ideal))
        for uid, ideal in ideals
    ]

    ideal_total = sum(ideal for _, ideal in ideals)
    floor_total = sum(f for _, f, _ in floors)
    extras = round(ideal_total) - floor_total

    # Sort by fractional remainder descending; top-N get +1
    floors.sort(key=lambda x: -x[2])

    result: list[tuple[int, int]] = [(burn_uid, MAX_WEIGHT)]
    for i, (uid, f, _rem) in enumerate(floors):
        u16_weight = f + (1 if i < extras else 0)
        if u16_weight <= 0:
            continue
        result.append((uid, u16_weight))

    return result
