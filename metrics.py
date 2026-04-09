import os
from collections import deque

try:
    from prometheus_client import (
        Counter,
        Gauge,
        start_http_server as _start_http_server,
    )

    _prometheus_available = True
except ImportError:
    _prometheus_available = False

_enabled = _prometheus_available and os.getenv(
    "METRICS_ENABLED", "true"
).lower() not in ("false", "0", "no")

# how many past epochs to keep in memory — cardinality = miners * retention * 7 gauges
# at 256 miners and 100 epochs that's ~180k label combos, well within reason
_EPOCH_RETENTION = int(os.getenv("METRICS_EPOCH_RETENTION", "100"))

# tracks insertion order; we evict the oldest when we exceed _EPOCH_RETENTION
_epoch_history: deque = deque()
# epoch_id -> set of (hotkey, uid) so we know exactly what to remove on eviction
_epoch_miners: dict[str, set[tuple[str, str]]] = {}


def _pad_epoch(epoch_id: str) -> str:
    """zero-pad numeric epoch ids so lexicographic sort == numeric sort in grafana"""
    try:
        return str(int(epoch_id)).zfill(8)
    except (ValueError, TypeError):
        return epoch_id


if _enabled:
    # --- epoch-level gauges (no epoch label — used for stat panels and live graphs) ---

    _epochs_processed = Counter(
        "validator_epochs_processed_total",
        "Epochs fully processed",
        ["weights_submitted"],
    )
    _epoch_total_cu = Gauge(
        "validator_epoch_total_cu", "Total CU in last processed epoch"
    )
    _epoch_miners_paid = Gauge(
        "validator_epoch_miners_paid", "Miners paid in last processed epoch"
    )
    _epoch_miners_banned = Gauge(
        "validator_epoch_miners_banned", "Miners banned in last processed epoch"
    )
    _epoch_burn_pct = Gauge(
        "validator_epoch_burn_pct", "Burn percentage in last processed epoch (0-1)"
    )
    _epoch_consumed_usd = Gauge(
        "validator_epoch_consumed_usd", "Total consumed USD in last processed epoch"
    )
    _alpha_price_usd = Gauge(
        "validator_alpha_price_usd", "Alpha token price in USD (last epoch)"
    )
    _tao_price_usd = Gauge("validator_tao_price_usd", "TAO price in USD (last epoch)")
    _emissions_alpha = Gauge(
        "validator_emissions_alpha", "Full epoch alpha emissions (last epoch)"
    )
    _current_epoch_number = Gauge(
        "validator_current_epoch_number", "Epoch number of the last processed epoch"
    )
    _blocks_since_last_update = Gauge(
        "validator_blocks_since_last_update",
        "Blocks elapsed since last weight submission on-chain",
    )
    _weight_submissions = Counter(
        "validator_weight_submissions_total",
        "Weight submission attempts by mode and outcome",
        ["mode", "success"],
    )

    # cumulative counters — lets Grafana compute totals over any time range with increase()
    _cu_consumed_total = Counter(
        "validator_cu_consumed_total",
        "Cumulative CU consumed across all epochs",
    )
    _usd_consumed_total = Counter(
        "validator_usd_consumed_total",
        "Cumulative USD consumed across all epochs",
    )

    # --- epoch-keyed price snapshots (epoch label only, bounded by _EPOCH_RETENTION)
    # used to join with per-miner history table in Grafana

    _alpha_price_epoch = Gauge(
        "validator_alpha_price_epoch",
        "Alpha price at the time of a specific epoch",
        ["epoch"],
    )
    _tao_price_epoch = Gauge(
        "validator_tao_price_epoch",
        "TAO price at the time of a specific epoch",
        ["epoch"],
    )

    # --- epoch-keyed aggregate gauges (bounded by _EPOCH_RETENTION)
    # these mirror the plain epoch gauges but carry an epoch label so
    # Grafana can render one-bar-per-epoch charts without relying on time bucketing

    _epoch_total_cu_hist = Gauge(
        "validator_epoch_total_cu_hist",
        "Total CU in the given epoch",
        ["epoch"],
    )
    _epoch_consumed_usd_hist = Gauge(
        "validator_epoch_consumed_usd_hist",
        "Total consumed USD in the given epoch",
        ["epoch"],
    )
    _epoch_burn_pct_hist = Gauge(
        "validator_epoch_burn_pct_hist",
        "Burn percentage in the given epoch (0-1)",
        ["epoch"],
    )

    # --- per-miner gauges with epoch label (bounded by _EPOCH_RETENTION) ---
    # instant query on these returns one series per epoch, giving clean per-epoch table rows

    _miner_cu = Gauge(
        "validator_miner_cu",
        "CU consumed by miner in the given epoch",
        ["hotkey", "uid", "epoch"],
    )
    _miner_weight = Gauge(
        "validator_miner_weight",
        "Weight fraction assigned to miner in the given epoch",
        ["hotkey", "uid", "epoch"],
    )
    _miner_price_bid = Gauge(
        "validator_miner_price_bid",
        "Miner target USD per CU in the given epoch",
        ["hotkey", "uid", "epoch"],
    )
    _miner_consumed_usd = Gauge(
        "validator_miner_consumed_usd",
        "USD consumed by miner in the given epoch",
        ["hotkey", "uid", "epoch"],
    )
    _miner_payout_usd = Gauge(
        "validator_miner_payout_usd",
        "USD paid out to miner in the given epoch",
        ["hotkey", "uid", "epoch"],
    )
    _miner_alpha_emissions = Gauge(
        "validator_miner_alpha_emissions",
        "Alpha token emissions allocated to miner in the given epoch (weight * total_emissions)",
        ["hotkey", "uid", "epoch"],
    )
    _miner_banned = Gauge(
        "validator_miner_banned",
        "Whether miner is banned (1) or active (0) in the given epoch",
        ["hotkey", "uid", "epoch"],
    )

    # --- verification loop ---

    _verification_epochs = Counter(
        "validator_verification_epochs_processed_total",
        "Epochs processed by the verification loop",
    )
    _verification_pass = Counter(
        "validator_verification_pass_total",
        "Passing verification checks",
        ["chain"],
    )
    _verification_fail = Counter(
        "validator_verification_fail_total",
        "Failing verification checks (triggers ban)",
        ["chain"],
    )
    _verification_bans = Counter(
        "validator_verification_bans_total",
        "Miners permanently banned after a verification failure",
    )
    _verification_sampled = Counter(
        "validator_verification_sampled_queries_total",
        "Queries sampled for verification across all miners",
    )
    _verification_skipped = Counter(
        "validator_verification_skipped_total",
        "Queries skipped during verification by reason",
        ["reason"],
    )
    _reference_failures = Counter(
        "validator_reference_node_failures_total",
        "Reference node query failures during verification",
    )


def start_metrics_server(port: int) -> None:
    if not _enabled:
        return
    _start_http_server(port)


def _evict_epoch(epoch_id: str) -> None:
    """Remove all label combos for an epoch that has aged out of the retention window."""
    miners = _epoch_miners.pop(epoch_id, set())
    for hotkey, uid in miners:
        for gauge in (
            _miner_cu,
            _miner_weight,
            _miner_price_bid,
            _miner_consumed_usd,
            _miner_payout_usd,
            _miner_alpha_emissions,
            _miner_banned,
        ):
            try:
                gauge.remove(hotkey, uid, epoch_id)
            except KeyError:
                pass
    for gauge in (_alpha_price_epoch, _tao_price_epoch):
        try:
            gauge.remove(epoch_id)
        except KeyError:
            pass
    for gauge in (_epoch_total_cu_hist, _epoch_consumed_usd_hist, _epoch_burn_pct_hist):
        try:
            gauge.remove(epoch_id)
        except KeyError:
            pass


# --- epoch-level helpers ---


def record_epoch(
    total_cu: int,
    miners_paid: int,
    miners_banned: int,
    burn_pct: float,
    consumed_usd: float,
    alpha_price: float,
    tao_price: float,
    emissions: float,
    weights_submitted: bool,
    epoch_id: str = "",
) -> None:
    if not _enabled:
        return
    _epochs_processed.labels(weights_submitted=str(weights_submitted).lower()).inc()
    _epoch_total_cu.set(total_cu)
    _epoch_miners_paid.set(miners_paid)
    _epoch_miners_banned.set(miners_banned)
    _epoch_burn_pct.set(burn_pct)
    _epoch_consumed_usd.set(consumed_usd)
    _alpha_price_usd.set(alpha_price)
    _tao_price_usd.set(tao_price)
    _emissions_alpha.set(emissions)
    _cu_consumed_total.inc(total_cu)
    _usd_consumed_total.inc(consumed_usd)
    try:
        _current_epoch_number.set(int(epoch_id))
    except (ValueError, TypeError):
        pass

    # epoch-keyed price snapshot for history table joins
    if epoch_id:
        ep = _pad_epoch(epoch_id)
        _alpha_price_epoch.labels(epoch=ep).set(alpha_price)
        _tao_price_epoch.labels(epoch=ep).set(tao_price)
        _epoch_total_cu_hist.labels(epoch=ep).set(total_cu)
        _epoch_consumed_usd_hist.labels(epoch=ep).set(consumed_usd)
        _epoch_burn_pct_hist.labels(epoch=ep).set(burn_pct)


def record_miner_epoch_data(
    miners: list,
    uid_map: dict,
    epoch_id: str,
    emissions_alpha: float,
) -> None:
    if not _enabled:
        return
    ep = _pad_epoch(epoch_id)

    # register this epoch and evict oldest if over retention limit
    if ep not in _epoch_miners:
        _epoch_history.append(ep)
        _epoch_miners[ep] = set()
        if len(_epoch_history) > _EPOCH_RETENTION:
            _evict_epoch(_epoch_history.popleft())

    for m in miners:
        uid = str(uid_map.get(m.miner_hotkey, "unknown"))
        _epoch_miners[ep].add((m.miner_hotkey, uid))
        alpha_emissions = m.weight * emissions_alpha
        _miner_cu.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(m.cu_total)
        _miner_weight.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(m.weight)
        _miner_price_bid.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(
            m.target_usd_per_cu
        )
        _miner_consumed_usd.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(
            m.consumed_usd
        )
        _miner_payout_usd.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(
            m.payout_usd
        )
        _miner_alpha_emissions.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(
            alpha_emissions
        )
        _miner_banned.labels(hotkey=m.miner_hotkey, uid=uid, epoch=ep).set(
            1 if m.is_banned else 0
        )


def record_blocks_since_update(blocks: int) -> None:
    if not _enabled:
        return
    _blocks_since_last_update.set(blocks)


def record_weight_submission(mode: str, success: bool) -> None:
    if not _enabled:
        return
    _weight_submissions.labels(mode=mode, success=str(success).lower()).inc()


# --- verification loop helpers ---


def record_verification_epoch() -> None:
    if not _enabled:
        return
    _verification_epochs.inc()


def record_verification_result(chain: str, is_correct: bool) -> None:
    if not _enabled:
        return
    if is_correct:
        _verification_pass.labels(chain=chain.upper()).inc()
    else:
        _verification_fail.labels(chain=chain.upper()).inc()


def record_verification_ban() -> None:
    if not _enabled:
        return
    _verification_bans.inc()


def record_verification_sample(count: int) -> None:
    if not _enabled:
        return
    _verification_sampled.inc(count)


def record_verification_skipped(reason: str, count: int = 1) -> None:
    if not _enabled:
        return
    _verification_skipped.labels(reason=reason).inc(count)


def record_reference_failure() -> None:
    if not _enabled:
        return
    _reference_failures.inc()
