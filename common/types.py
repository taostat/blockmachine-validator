from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Sequence
import hashlib
import json


MAX_WEIGHT = 65535


class Chain(Enum):
    TAO = "TAO"
    ETH = "ETH"
    BSC = "BSC"


@dataclass
class MinerInfo:
    uid: int
    hotkey: str
    coldkey: str


@dataclass
class QueryLog:
    id: int
    timestamp: datetime
    epoch_id: str
    chain: str
    method: str
    block_number: Optional[int]
    params: Optional[dict]
    miner_hotkey: str
    miner_coldkey: str
    node_id: str
    status_code: int
    response_hash: Optional[str]
    cu_cost: int
    latency_ms: int
    target_usd_per_cu: Optional[float] = None
    inferred_from_latest: Optional[bool] = None


@dataclass
class VerificationResult:
    is_correct: bool
    method: str
    params: list
    block_number: Optional[int] = None
    chain: Optional[str] = None
    node_id: Optional[str] = None
    source_query_id: Optional[int] = None
    miner_response_hash: Optional[str] = None
    ref_response_hash: Optional[str] = None
    latency_miner_ms: Optional[int] = None
    latency_ref_ms: Optional[int] = None
    miner_response: Optional[Any] = None
    ref_response: Optional[Any] = None
    error_details: Optional[str] = None


@dataclass
class MinerVerificationState:
    miner_hotkey: str
    first_seen_epoch: str
    total_logged_queries: int = 0
    last_logged_check: Optional[datetime] = None
    logged_pass_count: int = 0
    logged_fail_count: int = 0
    is_trusted: bool = False
    trusted_at: Optional[datetime] = None


@dataclass
class Incident:
    miner_hotkey: str
    miner_coldkey: str
    epoch_id: str
    verification_id: Optional[int] = None


@dataclass
class NodeEpochMetrics:
    epoch_id: str
    miner_hotkey: str
    node_id: str
    chain: Optional[str] = None

    total_responses: int = 0
    successful_responses: int = 0
    non200_responses: int = 0
    total_cu: int = 0

    latency_min_ms: Optional[int] = None
    latency_max_ms: Optional[int] = None
    latency_avg_ms: Optional[int] = None
    latency_p50_ms: Optional[int] = None
    latency_p95_ms: Optional[int] = None
    latency_p99_ms: Optional[int] = None

    last_head_block: Optional[int] = None

    accuracy_ok: bool = True
    success_rate: Optional[float] = None
    latency_score: Optional[float] = None
    quality_score: Optional[float] = None
    quality_ok: Optional[bool] = None


@dataclass
class QualityScoreResult:
    success_rate: float
    latency_score: float
    quality_score: float
    quality_ok: bool


@dataclass
class MinerEpochData:
    hotkey: str
    coldkey: str
    cu_total: int
    target_usd_per_cu: float
    is_blacklisted: bool = False
    cu_archive: int = 0
    cu_non_archive: int = 0
    price_archive: float = 0.0
    price_non_archive: float = 0.0


@dataclass
class MinerWeight:
    miner_hotkey: str
    cu_total: int
    target_usd_per_cu: float
    is_banned: bool
    consumed_usd: float = 0.0
    payout_usd: float = 0.0
    payout_alpha: float = 0.0
    weight: float = 0.0


@dataclass
class EpochWeightsResult:
    pool_usd_total: float
    pool_total_cu: int
    pool_usd_per_cu: float
    total_consumed_usd: float
    scale: float
    burn_alpha: float
    burn_pct: float
    burn_weight: float
    alpha_price_usd: float
    emissions_alpha: float = 0.0
    miners: list[MinerWeight] = field(default_factory=list)


@dataclass
class EpochInfo:
    epoch_id: str
    epoch_number: int
    period_start: datetime
    period_end: datetime


@dataclass
class MinerConfig:
    hotkey: str
    target_usd_per_cu: float


@dataclass
class GatewayCuAllocation:
    miner_hotkey: str
    request_type_cus: list[dict]
    total_cus: int


@dataclass
class EpochCuAllocationFile:
    epoch: int
    gateway_id: Optional[str]
    allocations: list[GatewayCuAllocation]
    finalized_at: Optional[str] = None


@dataclass
class EpochAudit:
    """write-only audit row saved each epoch for observability"""

    epoch_id: str
    block_number: int
    total_cu: int
    miners_paid: int
    miners_banned: int
    total_consumed_usd: float
    alpha_price_usd: float
    tao_price_usd: float
    emissions_alpha: float
    burn_pct: float
    weights_submitted: bool
    miner_details: dict = field(default_factory=dict)


def hash_response(response: Any, method: str | None = None) -> str:
    cleaned = _strip_nondeterministic(response, method)
    serialized = json.dumps(cleaned, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


# Methods whose responses contain fields that vary across nodes.
# chain_getBlock includes GRANDPA justifications that differ per node.
_NONDETERMINISTIC_FIELDS: dict[str, list[str]] = {
    "chain_getBlock": ["justifications"],
}


def _strip_nondeterministic(response: Any, method: str | None) -> Any:
    if method and method in _NONDETERMINISTIC_FIELDS and isinstance(response, dict):
        cleaned = {
            k: v
            for k, v in response.items()
            if k not in _NONDETERMINISTIC_FIELDS[method]
        }
        return cleaned
    return response


def normalize_hash(hash_value: Optional[str]) -> Optional[str]:
    if not hash_value:
        return None
    for prefix in ["sha256:", "md5:", "SHA256:", "MD5:"]:
        if hash_value.startswith(prefix):
            return hash_value[len(prefix) :]
    return hash_value


def hashes_match(hash1: Optional[str], hash2: Optional[str]) -> bool:
    if not hash1 or not hash2:
        return False
    return normalize_hash(hash1) == normalize_hash(hash2)


def percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_values) else f
    if f == c:
        return sorted_values[f]
    return sorted_values[f] * (c - k) + sorted_values[c] * (k - f)
