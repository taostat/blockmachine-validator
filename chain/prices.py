import asyncio
import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# tried in order until one succeeds; custom tao_price_api is prepended at runtime
_TAO_PRICE_APIS: list[tuple[str, str]] = [
    (
        "coingecko",
        "https://api.coingecko.com/api/v3/simple/price?ids=bittensor&vs_currencies=usd",
    ),
    (
        "coincap",
        "https://api.coincap.io/v2/assets/bittensor",
    ),
    (
        "binance",
        "https://api.binance.com/api/v3/ticker/price?symbol=TAOUSDT",
    ),
]


class AlphaPriceFetcher:
    def __init__(
        self,
        subtensor: Any,
        netuid: int,
        tao_price_api: Optional[str] = None,
    ):
        self.subtensor = subtensor
        self.netuid = netuid
        self.tao_price_api = tao_price_api
        self._http = httpx.AsyncClient(timeout=10)

        self._cached_tao_price: Optional[float] = None
        self._cached_alpha_price: Optional[float] = None
        self._cached_emissions: Optional[float] = None

    async def close(self):
        await self._http.aclose()

    async def get_alpha_price_usd(self) -> float:
        try:
            price_in_tao = await self._get_alpha_price_in_tao()
            tao_price_usd = await self.get_tao_price_usd()

            if price_in_tao > 0 and tao_price_usd > 0:
                alpha_price = price_in_tao * tao_price_usd
                self._cached_alpha_price = alpha_price
                logger.info(
                    f"Alpha price: ${alpha_price:.4f} "
                    f"(TAO=${tao_price_usd:.2f}, {price_in_tao:.6f} TAO/α)"
                )
                return alpha_price

        except Exception as e:
            logger.warning(f"Failed to fetch alpha price from chain: {e}")

        if self._cached_alpha_price is not None:
            logger.warning(
                f"Using last known alpha price: ${self._cached_alpha_price:.4f} (live fetch failed)"
            )
            return self._cached_alpha_price

        raise ValueError(
            "Cannot determine alpha price — live fetch failed and no cached value available"
        )

    async def _get_alpha_price_in_tao(self) -> float:
        if self.subtensor is None:
            return 0.0

        try:
            dynamic_info = await asyncio.to_thread(self.subtensor.subnet, self.netuid)
            if dynamic_info and hasattr(dynamic_info, "price"):
                return float(dynamic_info.price)
        except Exception as e:
            logger.debug(f"Could not get alpha price from subnet(): {e}")

        return 0.0

    async def get_tao_price_usd(self) -> float:
        apis: list[tuple[str, str]] = []
        if self.tao_price_api:
            apis.append(("custom", self.tao_price_api))
        apis.extend(_TAO_PRICE_APIS)

        errors: list[str] = []
        for source, url in apis:
            try:
                price = await self._fetch_tao_price(url)
                if price and price > 0:
                    if (
                        self._cached_tao_price is not None
                        and abs(price - self._cached_tao_price) / self._cached_tao_price
                        > 0.2
                    ):
                        logger.warning(
                            f"TAO price from {source} (${price:.2f}) deviates >20% "
                            f"from last known (${self._cached_tao_price:.2f}) — accepting anyway"
                        )
                    self._cached_tao_price = price
                    logger.debug(f"TAO price from {source}: ${price:.2f}")
                    return price
            except Exception as e:
                errors.append(f"{source}: {e}")
                logger.debug(f"TAO price fetch failed ({source}): {e}")

        if self._cached_tao_price is not None:
            logger.warning(
                f"All TAO price sources failed ({'; '.join(errors)}) — "
                f"using last known: ${self._cached_tao_price:.2f}"
            )
            return self._cached_tao_price

        raise ValueError(
            f"Cannot determine TAO price — all sources failed and no cached value available: {errors}"
        )

    async def _fetch_tao_price(self, url: str) -> Optional[float]:
        response = await self._http.get(url)
        response.raise_for_status()
        data = response.json()

        # coingecko
        if "bittensor" in data:
            return float(data["bittensor"].get("usd", 0))
        # coincap
        if "data" in data and isinstance(data["data"], dict):
            if "priceUsd" in data["data"]:
                return float(data["data"]["priceUsd"])
            if "price" in data["data"]:
                return float(data["data"]["price"])
        # binance
        if "price" in data and "symbol" in data:
            return float(data["price"])
        # generic
        if "price" in data:
            return float(data["price"])

        return None

    async def get_emissions_alpha(self) -> float:
        try:
            if self.subtensor is not None:
                dynamic_info = await asyncio.to_thread(
                    self.subtensor.subnet, self.netuid
                )
                tempo = await asyncio.to_thread(self.subtensor.tempo, self.netuid)

                if dynamic_info and hasattr(dynamic_info, "alpha_out_emission"):
                    emissions = float(dynamic_info.alpha_out_emission) * tempo
                    self._cached_emissions = emissions
                    logger.debug(
                        f"Full epoch emissions: {emissions:.2f}α "
                        f"({dynamic_info.alpha_out_emission}/block × {tempo} blocks)"
                    )
                    return emissions

        except Exception as e:
            logger.warning(f"Could not get emissions from chain: {e}")

        if self._cached_emissions is not None:
            logger.warning(
                f"Using last known emissions: {self._cached_emissions:.2f}α (live fetch failed)"
            )
            return self._cached_emissions

        raise ValueError(
            "Cannot determine emissions — live fetch failed and no cached value available"
        )
