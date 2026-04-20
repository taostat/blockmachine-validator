"""
BlockMachine Validator — production entry point.

    python -m validator.main

Runs independent async loops:
  1. Weight loop  — fetch CU allocations, compute weights, submit to chain
  2. Verification loop — sample S3 logs, verify against reference nodes, ban on failure
  3. Retention cleanup — periodic DB cleanup (only when DB is enabled)
"""

import asyncio
import logging
import os
import signal

import bittensor as bt

from validator.api.logs_client import LogsClient
from validator.api.registry_blacklist import RegistryBlacklistClient
from validator.api.registry_config import RegistryConfigClient
from validator.chain.bittensor import BittensorChain
from validator.chain.prices import AlphaPriceFetcher
from validator.chain.submitter import WeightSubmitter
from validator.config import ValidatorConfig, apply_registry_config, load_config
from validator.metrics import start_metrics_server
from validator.verification.loop import VerificationLoop
from validator.auth import TokenProvider
from validator.verification.reference import ReferenceNodeManager
from validator.weights.loop import WeightLoop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def _create_store(config: ValidatorConfig):
    """create DB store or NullStore based on config"""
    if not config.database.enabled:
        from validator.db.null_store import NullStore

        store = NullStore()
        await store.connect()
        return store

    from validator.db.postgres import PostgresStore

    store = PostgresStore(config.database)
    await store.connect()
    return store


async def _create_reference_manager(
    config: ValidatorConfig, token_provider: TokenProvider
) -> ReferenceNodeManager:
    """Build and authenticate the reference node manager."""
    gw = config.verification_gateway

    manager = ReferenceNodeManager(
        config.reference_nodes,
        timeout_ms=config.verification.reference_query_timeout_ms,
        gateway_url=gw.url,
        token_provider=token_provider if gw.url else None,
    )
    await manager.initialize()
    return manager


async def main():
    config_path = os.getenv("CONFIG_PATH")
    config = load_config(config_path)

    logger.info(
        f"Starting BlockMachine Validator (netuid={config.netuid}, network={config.network})"
    )

    start_metrics_server(config.metrics_port)
    logger.info(f"Prometheus metrics available on port {config.metrics_port}")

    # hotkey
    if config.wallet_hotkey_seed:
        hotkey = bt.Keypair.create_from_seed(config.wallet_hotkey_seed)
        logger.info("Hotkey loaded from seed (in-memory)")
    else:
        wallet = bt.Wallet(name=config.wallet_name, hotkey=config.wallet_hotkey)
        hotkey = wallet.hotkey
        logger.info("Hotkey loaded from wallet files on disk")

    # chain
    chain = BittensorChain(
        network=config.subtensor_url or config.network,
        netuid=config.netuid,
        hotkey=hotkey,
    )

    # shared auth — single TokenProvider for all registry/gateway interactions
    gw_auth = config.verification_gateway
    token_provider = TokenProvider(
        auth_url=gw_auth.auth_url,
        client_id=gw_auth.client_id,
        netuid=config.netuid,
        hotkey_ss58=chain.get_validator_hotkey(),
        sign_fn=chain.sign,
    )

    # fetch network-wide config from the registry (epoch, scoring,
    # reference nodes, S3 log location, CU schedule, …) and overlay it.
    registry_config_client = RegistryConfigClient(
        registry_url=config.registry_url,
        token_provider=token_provider,
    )
    try:
        remote_cfg = await registry_config_client.fetch()
        apply_registry_config(config, remote_cfg)
        logger.info("Fetched validator config from registry")
    except Exception as e:
        logger.warning(
            f"Failed to fetch validator config from registry: {e} — "
            "using local defaults"
        )
    finally:
        await registry_config_client.close()

    # prices — built after registry overlay so weights.price_* take effect
    price_netuid = config.weights.price_netuid or config.netuid
    price_subtensor = chain.get_subtensor()

    if config.weights.price_network and config.weights.price_network != config.network:
        try:
            price_subtensor = bt.Subtensor(network=config.weights.price_network)
            logger.info(
                f"Using separate subtensor for prices: {config.weights.price_network}"
            )
        except Exception as e:
            logger.warning(f"Could not create price subtensor: {e}, using validator's")

    prices = AlphaPriceFetcher(
        subtensor=price_subtensor,
        netuid=price_netuid,
        tao_price_api=config.weights.tao_price_api,
    )

    # registry client (epochs, miner configs, S3 log fetching)
    logs = LogsClient(
        s3_config=config.s3,
        registry_url=config.registry_url,
        token_provider=token_provider,
    )

    # blacklist
    blacklist = RegistryBlacklistClient(
        registry_url=config.registry_url,
        token_provider=token_provider,
    )

    # database (optional)
    store = await _create_store(config)

    # weight submission
    submitter = WeightSubmitter(chain, burn_sink_uid=config.weights.burn_sink_uid)

    # reference nodes for verification
    reference_manager = await _create_reference_manager(config, token_provider)

    # loops
    weight_loop = WeightLoop(
        config=config,
        chain=chain,
        logs=logs,
        blacklist=blacklist,
        store=store,
        prices=prices,
        submitter=submitter,
    )

    verification_loop = VerificationLoop(
        config=config.verification,
        logs=logs,
        chain=chain,
        blacklist=blacklist,
        store=store,
        reference_manager=reference_manager,
    )

    # retention cleanup (only with real DB)
    retention_task = None
    if config.database.enabled and hasattr(store, "run_retention_cleanup"):

        async def _retention_loop():
            while True:
                try:
                    await asyncio.sleep(3600)
                    await store.run_retention_cleanup(
                        config.database.data_retention_days
                    )
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning(f"Retention cleanup error (non-fatal): {e}")

        retention_task = asyncio.create_task(_retention_loop())
        logger.info(
            f"Data retention cleanup enabled (keeping {config.database.data_retention_days} days)"
        )

    # signal handling
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    async def shutdown():
        logger.info("Shutting down...")
        await weight_loop.stop()
        await verification_loop.stop()
        if retention_task:
            retention_task.cancel()
        await prices.close()
        await reference_manager.close()
        await logs.close()
        await blacklist.close()
        await token_provider.close()
        await store.close()
        stop_event.set()

    def signal_handler():
        asyncio.create_task(shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # run both loops
    logger.info("Validator initialized — starting weight loop + verification loop")
    tasks = [
        asyncio.create_task(weight_loop.run()),
        asyncio.create_task(verification_loop.run()),
    ]

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        pass
    finally:
        if not stop_event.is_set():
            await shutdown()

    logger.info("Validator stopped")


if __name__ == "__main__":
    asyncio.run(main())
