import os
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml


@dataclass
class DatabaseConfig:
    enabled: bool = True
    host: str = "localhost"
    port: int = 5432
    name: str = "blockmachine_validator"
    user: str = "validator"
    password: str = ""
    data_retention_days: int = 30


@dataclass
class S3Config:
    # Location fields below come from the registry (network-wide).
    bucket_url: str = ""
    bucket_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    prefix: str = ""
    region: str = "us-east-1"
    addressing_style: str = "auto"
    # Credentials stay per-deployment (env vars / secrets).
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None


@dataclass
class EpochConfig:
    start_block: int = 0
    epoch_length_blocks: int = 361
    buffer_blocks: int = 10


@dataclass
class WeightConfig:
    burn_sink_uid: int = 103
    tao_price_api: Optional[str] = None
    price_network: Optional[str] = None
    price_netuid: Optional[int] = None
    default_target_usd_per_cu: float = 0.0000005
    fallback_alpha_price_usd: Optional[float] = None
    fallback_emissions_alpha: Optional[float] = None


@dataclass
class VerificationConfig:
    logged_sample_pct: float = 1
    logged_max_samples_per_miner: int = 100000
    reference_query_timeout_ms: int = 10000


@dataclass
class ReferenceNodesConfig:
    # All values are provided by the registry.
    tao: str = ""
    eth: str = ""
    bsc: str = ""


@dataclass
class VerificationGatewayConfig:
    # All values are provided by the registry.
    url: str = ""
    auth_url: str = "https://test-auth.taostats.io"
    client_id: str = "07f5c729-5ca7-412a-b5e7-4966e132548e"


@dataclass
class ValidatorConfig:
    # Per-deployment identity / infra (env vars).
    netuid: int = 19
    network: str = "finney"
    wallet_name: str = "validator"
    wallet_hotkey: str = "default"
    wallet_hotkey_seed: Optional[str] = None
    registry_url: str = ""
    metrics_port: int = 9090
    metrics_epoch_retention: int = 100  # from registry

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    s3: S3Config = field(default_factory=S3Config)
    epoch: EpochConfig = field(default_factory=EpochConfig)
    weights: WeightConfig = field(default_factory=WeightConfig)
    verification: VerificationConfig = field(default_factory=VerificationConfig)
    reference_nodes: ReferenceNodesConfig = field(default_factory=ReferenceNodesConfig)
    verification_gateway: VerificationGatewayConfig = field(
        default_factory=VerificationGatewayConfig
    )

    # Network-wide CU schedule fetched from the registry.
    cu_schedule: dict = field(default_factory=dict)


def load_config(config_path: Optional[str] = None) -> ValidatorConfig:
    config = ValidatorConfig()

    if config_path and os.path.exists(config_path):
        with open(config_path) as f:
            data = yaml.safe_load(f)
            if data:
                _apply_dict(config, data)

    _apply_env(config)
    return config


def apply_registry_config(config: ValidatorConfig, data: dict) -> None:
    """Overlay network-wide values fetched from the registry.

    Called after ``load_config`` once the registry client is authenticated.
    Only consensus-critical / network-wide fields are touched; per-deployment
    fields (wallet, db, s3 credentials, registry_url, metrics_port) are left
    untouched.
    """
    if not data:
        return

    _apply_sub(config.epoch, data.get("epoch", {}))
    _apply_sub(config.weights, data.get("weights", {}))
    _apply_sub(config.verification, data.get("verification", {}))
    _apply_sub(config.reference_nodes, data.get("reference_nodes", {}))
    _apply_sub(config.verification_gateway, data.get("verification_gateway", {}))

    # S3 location only — credentials are not in the registry payload.
    s3 = data.get("s3", {}) or {}
    for k in (
        "bucket_url",
        "bucket_name",
        "endpoint_url",
        "prefix",
        "region",
        "addressing_style",
    ):
        if k in s3:
            setattr(config.s3, k, s3[k])

    chain = data.get("chain", {}) or {}
    if "network" in chain:
        config.network = chain["network"]

    metrics = data.get("metrics", {}) or {}
    if "epoch_retention" in metrics:
        config.metrics_epoch_retention = int(metrics["epoch_retention"])

    if "cu_schedule" in data:
        config.cu_schedule = data["cu_schedule"]


def _apply_dict(config: ValidatorConfig, data: dict):
    simple_fields = [
        "netuid",
        "network",
        "wallet_name",
        "wallet_hotkey",
        "wallet_hotkey_seed",
        "registry_url",
        "metrics_port",
    ]
    for f in simple_fields:
        if f in data:
            setattr(config, f, data[f])

    _apply_sub(config.database, data.get("database", {}))
    _apply_sub(config.s3, data.get("s3", {}))
    _apply_sub(config.epoch, data.get("epoch", {}))
    _apply_sub(config.weights, data.get("weights", {}))
    _apply_sub(config.verification, data.get("verification", {}))
    _apply_sub(config.reference_nodes, data.get("reference_nodes", {}))
    _apply_sub(config.verification_gateway, data.get("verification_gateway", {}))


def _apply_sub(obj: Any, data: dict):
    if not data:
        return
    for k, v in data.items():
        if hasattr(obj, k):
            setattr(obj, k, v)


def _apply_env(config: ValidatorConfig):
    # Per-deployment identity / infra.
    _env_int(config, "netuid", "NETUID")
    _env_str(config, "wallet_name", "WALLET_NAME")
    _env_str(config, "wallet_hotkey", "WALLET_HOTKEY")
    _env_str(config, "wallet_hotkey_seed", "WALLET_HOTKEY_SEED")
    _env_str(config, "registry_url", "REGISTRY_URL")
    _env_int(config, "metrics_port", "METRICS_PORT")

    # `network` is normally fetched from the registry, but allow an env
    # override for bootstrap (the validator must talk to a chain before it
    # can authenticate to the registry).
    _env_str(config, "network", "SUBTENSOR_NETWORK")

    gw = config.verification_gateway
    _env_str(gw, "auth_url", "GATEWAY_AUTH_URL")
    _env_str(gw, "client_id", "GATEWAY_CLIENT_ID")

    db = config.database
    _env_bool(db, "enabled", "DB_ENABLED")
    _env_str(db, "host", "LOCAL_DB_HOST")
    _env_int(db, "port", "LOCAL_DB_PORT")
    _env_str(db, "name", "LOCAL_DB_NAME")
    _env_str(db, "user", "LOCAL_DB_USER")
    _env_str(db, "password", "LOCAL_DB_PASSWORD")
    _env_int(db, "data_retention_days", "DB_RETENTION_DAYS", "DATA_RETENTION_DAYS")


def _env_str(obj, attr: str, *env_keys: str):
    for key in env_keys:
        val = os.getenv(key)
        if val is not None:
            setattr(obj, attr, val.strip("/") if attr == "prefix" else val)
            return


def _env_int(obj, attr: str, *env_keys: str):
    for key in env_keys:
        val = os.getenv(key)
        if val is not None:
            setattr(obj, attr, int(val))
            return


def _env_float(obj, attr: str, *env_keys: str):
    for key in env_keys:
        val = os.getenv(key)
        if val is not None:
            setattr(obj, attr, float(val))
            return


def _env_bool(obj, attr: str, *env_keys: str):
    for key in env_keys:
        val = os.getenv(key)
        if val is not None:
            setattr(obj, attr, val.lower() in ("true", "1", "yes"))
            return
