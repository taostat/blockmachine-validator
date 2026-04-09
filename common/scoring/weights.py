import logging

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
    cleaned: list[tuple[int, float]] = [
        (uid, w) for uid, w in miner_weights if w and w > 0
    ]

    if not cleaned:
        return [(burn_uid, MAX_WEIGHT)]

    total = sum(w for _, w in cleaned)
    if total <= 0:
        return [(burn_uid, MAX_WEIGHT)]

    renorm = total if total > 1.0 + 1e-9 else 1.0

    result: list[tuple[int, int]] = []
    running_total = 0

    for uid, weight in cleaned:
        u16_weight = int((weight / renorm) * MAX_WEIGHT)
        if u16_weight <= 0:
            continue
        result.append((uid, u16_weight))
        running_total += u16_weight

    remainder = MAX_WEIGHT - running_total
    if remainder <= 0:
        return result

    for i, (uid, w) in enumerate(result):
        if uid == burn_uid:
            result[i] = (uid, w + remainder)
            break
    else:
        result.append((burn_uid, remainder))

    return result
