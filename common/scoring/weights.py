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

    Weights are scaled against the LARGEST weight in the set, whichever
    entry that is. The chain applies its own max-normalization (scaling
    the largest submitted weight to 65535), so pinning our largest to
    MAX_WEIGHT makes that pass a no-op and the submitted proportions
    survive exactly.

    Burn is treated as an ordinary entry. It is not special-cased and it
    is not required to be present: when the caller passes no burn weight,
    the miners are simply normalized among themselves and nothing is
    burned.

    Previously burn was pinned to MAX_WEIGHT and every miner was scaled
    as ``(w / burn_weight) * MAX_WEIGHT``. That could not express two
    legitimate states: a zero burn returned ``[(burn_uid, MAX_WEIGHT)]``
    — 100% to burn, no miner paid — and any burn below the largest
    miner's share overflowed u16, because the ratio it scaled by
    exceeded 1. Normalizing against the maximum is equivalent whenever
    burn is the largest weight, so existing behaviour is preserved while
    the other states become representable.

    Rounding uses the largest-remainder method (as in
    proportional-representation elections) to minimise quantisation
    error.
    """
    cleaned: list[tuple[int, float]] = [
        (uid, w) for uid, w in miner_weights if w and w > 0
    ]

    # Nothing payable at all — no miners and no burn. Fail closed to a
    # full burn rather than submitting an empty weight vector: an epoch
    # with nothing to distribute must not silently pay someone.
    if not cleaned:
        return [(burn_uid, MAX_WEIGHT)]

    max_weight = max(w for _, w in cleaned)

    # Largest-remainder rounding:
    #   1. Compute the ideal (fractional) u16 value for each entry.
    #   2. Give each entry the floor of its ideal value.
    #   3. Distribute the leftover points to the largest remainders.
    ideals = [(uid, (w / max_weight) * MAX_WEIGHT) for uid, w in cleaned]

    floors = [
        (uid, math.floor(ideal), ideal - math.floor(ideal))
        for uid, ideal in ideals
    ]

    ideal_total = sum(ideal for _, ideal in ideals)
    floor_total = sum(f for _, f, _ in floors)
    extras = round(ideal_total) - floor_total

    # Sort by fractional remainder descending; top-N get +1
    floors.sort(key=lambda x: -x[2])

    result: list[tuple[int, int]] = []
    for i, (uid, f, _rem) in enumerate(floors):
        u16_weight = f + (1 if i < extras else 0)
        if u16_weight <= 0:
            continue
        result.append((uid, min(u16_weight, MAX_WEIGHT)))

    # Defensive: if rounding somehow eliminated every entry, burn rather
    # than submit nothing.
    if not result:
        return [(burn_uid, MAX_WEIGHT)]

    return result
