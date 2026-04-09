from typing import TypedDict


class NormalizedMinerConfig(TypedDict):
    price_non_archive: float
    price_archive: float


def normalize_miner_config(cfg: object, default: float) -> NormalizedMinerConfig:
    """Convert flat or enriched miner config to canonical form."""

    def _extract_price(section: object, fallback: float) -> float:
        if not isinstance(section, dict):
            return fallback
        if "target_usd_per_cu" in section:
            return section["target_usd_per_cu"]
        if "target_usd_per_million_cu" in section:
            return section["target_usd_per_million_cu"] / 1_000_000
        return fallback

    if cfg is None:
        return {"price_non_archive": default, "price_archive": default}

    if isinstance(cfg, dict):
        has_default = "default" in cfg and isinstance(cfg.get("default"), dict)
        has_archive = "archive" in cfg and isinstance(cfg.get("archive"), dict)

        if has_default or has_archive:
            price_non_archive = (
                _extract_price(cfg["default"], default) if has_default else default
            )
            price_archive = (
                _extract_price(cfg["archive"], default) if has_archive else default
            )
            return {
                "price_non_archive": price_non_archive,
                "price_archive": price_archive,
            }

        if "target_usd_per_cu" in cfg:
            price = cfg["target_usd_per_cu"]
            return {"price_non_archive": price, "price_archive": price}

        if "target_usd_per_million_cu" in cfg:
            price = cfg["target_usd_per_million_cu"] / 1_000_000
            return {"price_non_archive": price, "price_archive": price}

    if hasattr(cfg, "target_usd_per_cu"):
        price = cfg.target_usd_per_cu
        return {"price_non_archive": price, "price_archive": price}

    if hasattr(cfg, "target_usd_per_million_cu"):
        price = cfg.target_usd_per_million_cu / 1_000_000
        return {"price_non_archive": price, "price_archive": price}

    return {"price_non_archive": default, "price_archive": default}
