"""EVM response canonicalization (July 2026 false-ban RCA).

Same-block responses from different execution clients differ in
presentation-only fields: geth >= 1.16 / erigon >= 3.5 add
`blockTimestamp` to every tx object (execution-apis addition) while
reth <= 2.1 omits it, and client builds disagree on the computed `size`
field (observed one-byte skew between geth-bsc builds). 14 of the 15
ban signals raised in July 2026 reproduced byte-for-byte from these two
differences alone. Hashing must strip them on both the validator and the
gateway (gateway/src/hash_utils.rs) so honest miners on a different
client than the reference never mismatch.
"""

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

from validator.common.types import (
    Chain,
    MinerInfo,
    QueryLog,
    hash_response,
    hash_response_variants,
)
from validator.config import VerificationConfig
from validator.verification.logged import LoggedVerifier


def _reth_block(full_tx: bool = True) -> dict:
    txs: list = (
        [
            {"hash": "0xaa", "type": "0x2", "value": "0x1"},
            {"hash": "0xbb", "type": "0x0", "value": "0x2"},
        ]
        if full_tx
        else ["0xaa", "0xbb"]
    )
    return {
        "hash": "0xblockhash",
        "number": "0x187c346",
        "timestamp": "0x6a7092af",
        "size": "0x123f2",
        "transactions": txs,
    }


def _geth_block(full_tx: bool = True) -> dict:
    # geth >= 1.16 / erigon >= 3.5 serialization of the same block: identical
    # to reth's except every tx object carries blockTimestamp (as observed in
    # all 13 eth ban-signal reproductions).
    block = _reth_block(full_tx)
    if full_tx:
        block["transactions"] = [
            {**tx, "blockTimestamp": block["timestamp"]}
            for tx in block["transactions"]
        ]
    return block


def test_cross_language_vector():
    # Pinned digest shared with the gateway's Rust hasher
    # (gateway/src/hash_utils.rs test_evm_block_hash_matches_validator_vector);
    # both sides must produce it for this block.
    assert (
        hash_response(_geth_block(), "eth_getBlockByNumber")
        == "04443e1109a55bddaf54b8bfba0917baabee9ca13e9dd05d2d46d98874e1683f"
    )


def test_client_specific_block_fields_hash_equal():
    for method in ("eth_getBlockByNumber", "eth_getBlockByHash"):
        assert hash_response(_geth_block(), method) == hash_response(
            _reth_block(), method
        )


def test_size_skew_hashes_equal():
    # bsc signal 315: identical block, size differed by one byte
    a = _reth_block(full_tx=False)
    b = _reth_block(full_tx=False)
    b["size"] = "0x123f3"
    b["milliTimestamp"] = "0x1a2b3c"
    b["totalDifficulty"] = "0xc70d815d562d3cfa955"
    assert hash_response(a, "eth_getBlockByNumber") == hash_response(
        b, "eth_getBlockByNumber"
    )


def test_hash_only_tx_lists_pass_through():
    blk = _reth_block(full_tx=False)
    stripped = hash_response(blk, "eth_getBlockByNumber")
    assert stripped == hash_response(_geth_block(full_tx=False), "eth_getBlockByNumber")
    # tx hashes themselves still matter
    other = _reth_block(full_tx=False)
    other["transactions"] = ["0xaa", "0xcc"]
    assert stripped != hash_response(other, "eth_getBlockByNumber")


def test_tx_by_index_strips_block_timestamp():
    reth_tx = {"hash": "0xaa", "type": "0x2", "value": "0x1"}
    geth_tx = {**reth_tx, "blockTimestamp": "0x6a7092af"}
    for method in (
        "eth_getTransactionByBlockNumberAndIndex",
        "eth_getTransactionByBlockHashAndIndex",
        "eth_getTransactionByHash",
    ):
        assert hash_response(geth_tx, method) == hash_response(reth_tx, method)


def test_consensus_fields_still_verified():
    a = _reth_block()
    b = _reth_block()
    b["transactions"][0]["value"] = "0xdead"
    assert hash_response(a, "eth_getBlockByNumber") != hash_response(
        b, "eth_getBlockByNumber"
    )


def test_substrate_justifications_still_stripped():
    a = {"block": {"header": {}}, "justifications": ["j1"]}
    b = {"block": {"header": {}}, "justifications": ["j2"]}
    assert hash_response(a, "chain_getBlock") == hash_response(b, "chain_getBlock")


def test_unlisted_methods_hash_raw():
    a = {"size": "0x1"}
    b = {"size": "0x2"}
    assert hash_response(a, "eth_call") != hash_response(b, "eth_call")


def test_variants_cover_legacy_gateway_hashes():
    reth = _reth_block()
    variants = hash_response_variants(reth, "eth_getBlockByNumber")

    # canonical first — what a canonicalizing gateway reports
    assert variants[0] == hash_response(reth, "eth_getBlockByNumber")
    # legacy raw — old gateway, miner on the reference's own client
    assert hash_response(reth) in variants
    # legacy raw + per-tx blockTimestamp — old gateway, geth/erigon miner
    geth_raw = hash_response(_geth_block())
    assert geth_raw not in (variants[0], hash_response(reth))
    assert geth_raw in variants

    # reverse direction: geth/erigon reference, old gateway + reth miner
    geth_ref_variants = hash_response_variants(_geth_block(), "eth_getBlockByNumber")
    assert hash_response(reth) in geth_ref_variants


def test_variants_for_unlisted_method_is_single_hash():
    assert hash_response_variants({"a": 1}, "eth_call") == [
        hash_response({"a": 1}, "eth_call")
    ]


class _BlockReferenceManager:
    """Reference returning a reth-style block for any query."""

    def __init__(self, block: dict):
        self.block = block

    async def query(self, chain: str, method: str, params: list):
        return self.block

    async def get_block_hash(self, chain: str, block_number: int) -> str:
        return f"0x{block_number:064x}"


def _block_log(response_hash: str) -> QueryLog:
    return QueryLog(
        id=126,
        timestamp=datetime.now(timezone.utc),
        epoch_id="test",
        chain=Chain.ETH.value,
        method="eth_getBlockByNumber",
        block_number=25674566,
        params={"params": ["0x187c346", True]},
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
async def test_geth_miner_vs_reth_reference_is_correct():
    # Regression for ban signals 291-321: pinned-block query, miner hash
    # produced by an old (raw-hashing) gateway from a geth response, the
    # reference serving the identical block in reth's serialization.
    ref = _BlockReferenceManager(_reth_block())
    verifier = LoggedVerifier(VerificationConfig(), ref)
    miner = MinerInfo(uid=0, hotkey="miner-hotkey", coldkey="miner-coldkey")

    legacy_gateway_hash = f"sha256:{hash_response(_geth_block())}"
    results = await verifier.verify(
        miner, [_block_log(legacy_gateway_hash)], sample_pct=1.0, max_samples=1
    )

    assert len(results) == 1
    assert results[0].is_correct


@pytest.mark.asyncio
async def test_wrong_block_content_still_fails():
    tampered = _reth_block()
    tampered["transactions"][0]["value"] = "0xdead"
    ref = _BlockReferenceManager(_reth_block())
    verifier = LoggedVerifier(VerificationConfig(), ref)
    miner = MinerInfo(uid=0, hotkey="miner-hotkey", coldkey="miner-coldkey")

    miner_hash = f"sha256:{hash_response(tampered, 'eth_getBlockByNumber')}"
    results = await verifier.verify(
        miner, [_block_log(miner_hash)], sample_pct=1.0, max_samples=1
    )

    assert len(results) == 1
    assert not results[0].is_correct
