"""v0.2.13: three ways a validator could lose an epoch to one failed request.

THE INCIDENT (uid 241, mainnet, 2026-09-10). A redeploy; the first fetch of
the registry config failed; the code logged "using local defaults" and
carried on. The local default for the log bucket's endpoint is nothing. The
S3 client parsed its endpoint once at construction, so when the hourly
refresh later succeeded the client never saw it. Every read of the epoch's
traffic data failed with boto's `Invalid endpoint: `. Thirty minutes after
the first attempt, the weight loop's timeout rule submitted 100% of the
validator's weight to the burn address. validator_trust 0, dividends 0, on
9% of the subnet's stake. uid 65 had been sitting in the same end state
since August.

Three changes, one per layer, each pinned here:

1. A failed startup config fetch RETRIES until it succeeds. It never falls
   through to empty settings.
2. The S3 client follows the live config. A refreshed endpoint reaches it;
   an empty endpoint is refused with a message that says what is wrong.
3. When the traffic data cannot be read, the loop RE-SENDS the vector
   already on chain. It never burns. A brand-new validator with nothing on
   chain submits nothing.

Run: python3 -m pytest tests/test_no_burn_on_timeout.py
"""

import asyncio
import importlib.util
import sys
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

if "bittensor" not in sys.modules:
    bt_stub = types.ModuleType("bittensor")
    bt_stub.Keypair = type("Keypair", (), {})
    sys.modules["bittensor"] = bt_stub

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

from validator.api.s3_repository import S3Repository
from validator.chain.submitter import WeightSubmitter
from validator.config import S3Config, ValidatorConfig
from validator.weights.loop import WeightLoop, _CU_RETRY_TIMEOUT

PREVIOUS = [(178, 65535), (129, 46026), (237, 21928), (108, 5)]


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 3. the timeout re-sends, never burns
# ---------------------------------------------------------------------------


def _loop(own_weights, resubmit_result=True):
    loop = WeightLoop.__new__(WeightLoop)
    loop.config = ValidatorConfig()
    loop.chain = types.SimpleNamespace(
        get_current_block=AsyncMock(return_value=9_039_281),
        get_tempo=AsyncMock(return_value=7200),
        get_last_epoch_block=AsyncMock(return_value=9_032_129),
        get_own_weights=AsyncMock(return_value=own_weights),
    )
    loop.store = types.SimpleNamespace(mark_epoch_processed=AsyncMock())
    loop.submitter = types.SimpleNamespace(
        submit=AsyncMock(return_value=True),
        resubmit_previous=AsyncMock(return_value=resubmit_result),
        last_commit_block=9_039_281,
    )
    loop.logs = types.SimpleNamespace(
        fetch_cu_allocations_from_manifest=AsyncMock(return_value={})
    )
    loop._cu_retry_start = {"9024961": time.time() - _CU_RETRY_TIMEOUT - 1}
    loop._last_submitted_block = None
    loop._reveal_offset_blocks = 3
    return loop


def test_a_data_timeout_resends_the_vector_already_on_chain_and_never_burns():
    """THE BUG THAT COST UID 241 ITS EPOCH. The timeout used to call
    `_submit_direct([], 1.0)`, which normalizes to [(burn, 65535)]. Now it
    re-sends what the chain already holds for us, exactly."""
    loop = _loop(own_weights=PREVIOUS)
    done = run(loop._process_epoch("9024961", {"hk": {}}, manifest={"gw": {}}))
    assert done is True
    loop.submitter.resubmit_previous.assert_awaited_once()
    sent = loop.submitter.resubmit_previous.await_args.args[0]
    assert sent == PREVIOUS, "the re-sent vector is not the one on chain"
    loop.submitter.submit.assert_not_awaited()  # no burn went out
    loop.store.mark_epoch_processed.assert_awaited_once_with(
        "9024961", weights_submitted=True
    )
    assert loop._last_submitted_block == 9_039_281
    assert "9024961" not in loop._cu_retry_start


def test_a_brand_new_validator_with_nothing_on_chain_submits_nothing():
    """No data and no previous vector: it has no income to protect and
    nothing honest to say. Waiting is correct; burning was not."""
    loop = _loop(own_weights=[])
    done = run(loop._process_epoch("9024961", {"hk": {}}, manifest={"gw": {}}))
    assert done is False
    loop.submitter.resubmit_previous.assert_not_awaited()
    loop.submitter.submit.assert_not_awaited()
    loop.store.mark_epoch_processed.assert_not_awaited()
    assert "9024961" in loop._cu_retry_start, "the expired timer must survive"


def test_an_unreadable_chain_submits_nothing_and_tries_again_next_cycle():
    loop = _loop(own_weights=None)
    done = run(loop._process_epoch("9024961", {"hk": {}}, manifest={"gw": {}}))
    assert done is False
    loop.submitter.resubmit_previous.assert_not_awaited()
    loop.submitter.submit.assert_not_awaited()
    assert "9024961" in loop._cu_retry_start


def test_a_chain_that_cannot_report_own_weights_does_not_burn_either():
    """A ChainInterface predating `get_own_weights` (or a test double) must
    degrade to 'not committing', never to the old burn."""
    loop = _loop(own_weights=PREVIOUS)
    loop.chain = types.SimpleNamespace(
        get_current_block=AsyncMock(return_value=9_039_281),
        get_tempo=AsyncMock(return_value=7200),
        get_last_epoch_block=AsyncMock(return_value=9_032_129),
    )
    done = run(loop._process_epoch("9024961", {"hk": {}}, manifest={"gw": {}}))
    assert done is False
    loop.submitter.submit.assert_not_awaited()


def test_a_failed_resend_leaves_the_epoch_unprocessed_with_the_timer_expired():
    loop = _loop(own_weights=PREVIOUS, resubmit_result=False)
    done = run(loop._process_epoch("9024961", {"hk": {}}, manifest={"gw": {}}))
    assert done is False
    loop.store.mark_epoch_processed.assert_not_awaited()
    assert "9024961" in loop._cu_retry_start


def test_the_finalizer_declaring_a_burn_still_burns():
    """Not every burn is a fault. `miner_configs == {}` is the epoch
    finalizer stating there was no gateway activity; that is its call and
    every validator must converge on it. Only the DATA-UNREADABLE path
    changed."""
    loop = _loop(own_weights=PREVIOUS)
    loop._submit_direct = AsyncMock(return_value=True)
    done = run(loop._process_epoch("9024961", {}))
    assert done is True
    loop._submit_direct.assert_awaited_once_with([], 1.0)
    loop.submitter.resubmit_previous.assert_not_awaited()


def test_the_data_path_never_reaches_the_burn_helper():
    """Structural: with data unreadable, `_submit_direct` (the only thing
    that can produce [(burn, 65535)] from nothing) is never called."""
    loop = _loop(own_weights=PREVIOUS)
    loop._submit_direct = AsyncMock(return_value=True)
    run(loop._process_epoch("9024961", {"hk": {}}, manifest={"gw": {}}))
    loop._submit_direct.assert_not_awaited()


# ---------------------------------------------------------------------------
# 3b. the re-send reproduces the on-chain vector exactly
# ---------------------------------------------------------------------------


class RecordingChain:
    def __init__(self):
        self.calls = []

    async def get_current_block(self):
        return 9_039_281

    async def set_weights(self, uids, weights, block_time=None):
        self.calls.append((list(uids), list(weights), block_time))
        return True

    async def verify_weight_commit_landed(self, since_block):
        return {"landed": True, "commit_block": since_block}


def test_resubmit_sends_the_same_uids_and_u16_values():
    """Chain values are already max-normalized, so they survive `submit`'s
    normalization unchanged. If this ever drifts, a re-send would quietly
    alter the validator's vote."""
    chain = RecordingChain()
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.resubmit_previous(PREVIOUS, block_time=11.9)) is True
    uids, values, bt = chain.calls[0]
    assert uids == [u for u, _ in PREVIOUS]
    assert values == [w for _, w in PREVIOUS], values
    assert bt == 11.9


def test_resubmit_refuses_an_empty_vector():
    chain = RecordingChain()
    submitter = WeightSubmitter(chain, ValidatorConfig().weights)
    assert run(submitter.resubmit_previous([])) is False
    assert chain.calls == []


# ---------------------------------------------------------------------------
# 2. the S3 client follows the live config
# ---------------------------------------------------------------------------


class FakeS3Client:
    def __init__(self, endpoint, style):
        self.endpoint = endpoint
        self.style = style

    def list_objects_v2(self, **kw):
        return {"Contents": []}


def _repo_with_fake_boto(cfg):
    repo = S3Repository(cfg)
    built = []

    def build(sig, style):
        client = FakeS3Client(repo._endpoint_url, style)
        built.append(client)
        return client

    repo._build_client = build
    return repo, built


def test_a_refreshed_endpoint_reaches_the_s3_client():
    """THE SECOND HALF OF THE INCIDENT. The endpoint was parsed once at
    construction; the hourly refresh brought the real one and the client
    never saw it."""
    cfg = S3Config()  # startup with nothing — the failed-fetch state
    repo, built = _repo_with_fake_boto(cfg)
    with pytest.raises(RuntimeError):
        repo._get_s3()
    # The refresh lands, mutating the SAME config object in place.
    cfg.bucket_name = "blockmachine-gateway-logs-mainnet"
    cfg.endpoint_url = "https://s3.gra.io.cloud.ovh.net"
    client = repo._get_s3()
    assert client.endpoint == "https://s3.gra.io.cloud.ovh.net", client.endpoint
    # And a later change rebuilds again.
    cfg.endpoint_url = "https://elsewhere.example"
    client2 = repo._get_s3()
    assert client2 is not client
    assert client2.endpoint == "https://elsewhere.example"
    assert len(built) == 2


def test_an_unchanged_config_reuses_the_client():
    cfg = S3Config(bucket_name="b", endpoint_url="https://s3.example")
    repo, built = _repo_with_fake_boto(cfg)
    assert repo._get_s3() is repo._get_s3()
    assert len(built) == 1


def test_an_empty_endpoint_is_refused_with_a_message_that_says_so():
    """boto accepts an empty endpoint and fails per call with
    `Invalid endpoint: ` — the exact line uid 241's operator was reading."""
    repo, _ = _repo_with_fake_boto(S3Config())
    with pytest.raises(RuntimeError) as e:
        repo._get_s3()
    assert "not configured" in str(e.value)
    assert "registry config" in str(e.value)


# ---------------------------------------------------------------------------
# 1. the startup fetch never falls through
# ---------------------------------------------------------------------------


def test_the_startup_fetch_retries_until_the_registry_answers():
    """THE FIRST HALF OF THE INCIDENT. One failed request used to mean
    'using local defaults' — and the default endpoint is nothing."""
    from validator.main import fetch_registry_config_or_wait

    class FlakyClient:
        def __init__(self, failures):
            self.failures = failures
            self.calls = 0

        async def fetch(self):
            self.calls += 1
            if self.calls <= self.failures:
                raise ConnectionError("registry unreachable")
            return {"s3": {"bucket_name": "b", "endpoint_url": "https://s3.example"}}

    slept = []

    async def fake_sleep(secs):
        slept.append(secs)

    client = FlakyClient(failures=3)
    config = ValidatorConfig()
    run(fetch_registry_config_or_wait(client, config, sleep=fake_sleep))
    assert client.calls == 4
    assert slept == [5, 10, 20], slept
    assert config.s3.endpoint_url == "https://s3.example", (
        "the config was not applied after the fetch finally succeeded"
    )


def test_the_startup_fetch_does_not_return_while_failing():
    """It must never proceed with empty settings. Bounded here by making
    sleep raise after a few rounds; in production it keeps going."""
    from validator.main import fetch_registry_config_or_wait

    class DeadClient:
        calls = 0

        async def fetch(self):
            self.calls += 1
            raise ConnectionError("registry unreachable")

    class Enough(Exception):
        pass

    rounds = []

    async def fake_sleep(secs):
        rounds.append(secs)
        if len(rounds) >= 7:
            raise Enough()

    config = ValidatorConfig()
    with pytest.raises(Enough):
        run(fetch_registry_config_or_wait(DeadClient(), config, sleep=fake_sleep))
    assert config.s3.endpoint_url is None, "proceeded with local defaults"
    assert rounds[-1] == 60, "backoff should cap at 60s"
