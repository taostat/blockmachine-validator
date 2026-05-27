from datetime import datetime, timezone
import importlib.util
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
try:
    import validator  # noqa: F401
except ModuleNotFoundError:
    spec = importlib.util.spec_from_file_location(
        "validator",
        _REPO / "__init__.py",
        submodule_search_locations=[str(_REPO)],
    )
    assert spec and spec.loader, "could not bootstrap validator package"
    mod = importlib.util.module_from_spec(spec)
    sys.modules["validator"] = mod
    spec.loader.exec_module(mod)

from validator.common.types import Chain, MinerInfo, QueryLog, hash_response
from validator.config import VerificationConfig
from validator.verification.logged import LoggedVerifier
from validator.verification.method_registry import is_verifiable


class DummyReferenceManager:
    def __init__(self):
        self.queries = []

    async def query(self, chain: str, method: str, params: list):
        self.queries.append((chain, method, list(params)))
        return {"method": method, "params": params}

    async def get_block_hash(self, chain: str, block_number: int) -> str:
        return f"0x{block_number:064x}"


def _log(method: str, params: list, response_hash: str, block_number: int | None):
    return QueryLog(
        id=1,
        timestamp=datetime.now(timezone.utc),
        epoch_id="test",
        chain=Chain.ETH.value,
        method=method,
        block_number=block_number,
        params={"params": params},
        miner_hotkey="miner-hotkey",
        miner_coldkey="miner-coldkey",
        node_id="node-1",
        status_code=200,
        response_hash=response_hash,
        cu_cost=1,
        latency_ms=1,
        inferred_from_latest=False,
    )


@pytest.mark.asyncio
async def test_eth_latest_block_tag_uses_logged_block_tolerance():
    ref = DummyReferenceManager()
    verifier = LoggedVerifier(VerificationConfig(), ref)
    miner = MinerInfo(uid=0, hotkey="miner-hotkey", coldkey="miner-coldkey")

    pinned_params = ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "0x64"]
    miner_hash = hash_response(
        {"method": "eth_getBalance", "params": pinned_params},
        "eth_getBalance",
    )
    log = _log(
        "eth_getBalance",
        ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "latest"],
        miner_hash,
        block_number=100,
    )

    results = await verifier.verify(miner, [log], sample_pct=1.0, max_samples=1)

    assert len(results) == 1
    assert results[0].is_correct
    assert results[0].used_block_tolerance
    assert ref.queries[0][2] == pinned_params


@pytest.mark.asyncio
async def test_eth_pending_block_tag_is_skipped():
    ref = DummyReferenceManager()
    verifier = LoggedVerifier(VerificationConfig(), ref)
    miner = MinerInfo(uid=0, hotkey="miner-hotkey", coldkey="miner-coldkey")
    miner_hash = hash_response({"result": "pending"}, "eth_getBalance")
    log = _log(
        "eth_getBalance",
        ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045", "pending"],
        miner_hash,
        block_number=100,
    )

    results = await verifier.verify(miner, [log], sample_pct=1.0, max_samples=1)

    assert results == []
    assert ref.queries == []


def test_eth_method_registry_rollout_safety():
    # eth_getProof is non-verifiable until MPT-based proof verification lands;
    # reth and erigon return semantically-equivalent but byte-distinct proofs,
    # so hash-equality against a single reference client would false-ban miners.
    assert not is_verifiable(Chain.ETH.value, "eth_getProof")
    assert not is_verifiable(Chain.ETH.value, "eth_getTransactionByHash")
    assert not is_verifiable(Chain.ETH.value, "eth_getTransactionReceipt")
