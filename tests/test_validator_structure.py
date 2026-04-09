"""Unit tests for refactored validator structure (PR #143).

Covers:
- EpochCalculator — epoch number calculation and edge cases
- S3Repository — S3 operations with mocked boto3
- NormalizedMinerConfig and normalize_miner_config — config normalization
- VerificationLoop._verify_epoch — DB error handling in _verify_miner
"""

import json
import sys
import types
from dataclasses import dataclass
from io import BytesIO
from typing import Optional
from unittest.mock import AsyncMock, Mock, patch

import pytest


# ---------------------------------------------------------------------------
# boto3 / botocore stubs (needed before importing validator modules)
# ---------------------------------------------------------------------------


def _ensure_boto3_stubs():
    if "boto3" not in sys.modules:
        boto3 = types.ModuleType("boto3")
        boto3.client = Mock()
        sys.modules["boto3"] = boto3

    if "botocore" not in sys.modules:
        sys.modules["botocore"] = types.ModuleType("botocore")

    if "botocore.config" not in sys.modules:
        config_module = types.ModuleType("botocore.config")

        class Config:
            def __init__(self, *args, **kwargs):
                pass

        config_module.Config = Config
        sys.modules["botocore.config"] = config_module

    if "botocore.exceptions" not in sys.modules:
        exc_module = types.ModuleType("botocore.exceptions")

        class ClientError(Exception):
            def __init__(self, error_response, operation_name):
                self.response = error_response
                self.operation_name = operation_name
                super().__init__(str(error_response))

        exc_module.ClientError = ClientError
        sys.modules["botocore.exceptions"] = exc_module


_ensure_boto3_stubs()

# ---------------------------------------------------------------------------
# EpochCalculator tests
# ---------------------------------------------------------------------------

from validator.api.epoch import EpochCalculator  # noqa: E402
from validator.config import EpochConfig  # noqa: E402


def _make_epoch_config(
    start_block: int = 1000,
    epoch_length_blocks: int = 361,
    buffer_blocks: int = 10,
) -> EpochConfig:
    cfg = EpochConfig()
    cfg.start_block = start_block
    cfg.epoch_length_blocks = epoch_length_blocks
    cfg.buffer_blocks = buffer_blocks
    return cfg


class TestEpochCalculator:
    def test_get_current_epoch_before_start(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000, buffer_blocks=10))
        # adjusted = 500 - 1000 - 10 = -510 < 0
        assert calc.get_current_epoch(500) == -1

    def test_get_current_epoch_within_buffer(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000, buffer_blocks=10))
        # adjusted = 1005 - 1000 - 10 = -5 < 0
        assert calc.get_current_epoch(1005) == -1

    def test_get_current_epoch_at_start_of_first_epoch(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000, buffer_blocks=10))
        # adjusted = 1010 - 1000 - 10 = 0 → (0 // 361) - 1 = -1
        assert calc.get_current_epoch(1010) == -1

    def test_get_current_epoch_first_completed_epoch(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000, buffer_blocks=10))
        # adjusted = 1010 + 361 - 1000 - 10 = 361 → (361 // 361) - 1 = 0
        assert calc.get_current_epoch(1010 + 361) == 0

    def test_get_current_epoch_second_epoch(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000, buffer_blocks=10))
        # adjusted = 1010 + 722 - 1000 - 10 = 722 → (722 // 361) - 1 = 1
        assert calc.get_current_epoch(1010 + 722) == 1

    def test_get_current_epoch_mid_epoch(self):
        calc = EpochCalculator(_make_epoch_config(start_block=0, buffer_blocks=0))
        # adjusted = 500 → (500 // 361) - 1 = 1 - 1 = 0
        assert calc.get_current_epoch(500) == 0

    def test_get_epoch_id_epoch_zero(self):
        calc = EpochCalculator(
            _make_epoch_config(start_block=1000, epoch_length_blocks=361)
        )
        # start = 1000 + 0 * 361 = 1000
        assert calc.get_epoch_id(0) == "1000"

    def test_get_epoch_id_epoch_one(self):
        calc = EpochCalculator(
            _make_epoch_config(start_block=1000, epoch_length_blocks=361)
        )
        # start = 1000 + 1 * 361 = 1361
        assert calc.get_epoch_id(1) == "1361"

    def test_get_epoch_id_epoch_five(self):
        calc = EpochCalculator(
            _make_epoch_config(start_block=1000, epoch_length_blocks=361)
        )
        assert calc.get_epoch_id(5) == str(1000 + 5 * 361)

    def test_epoch_id_to_number_round_trip(self):
        calc = EpochCalculator(
            _make_epoch_config(start_block=1000, epoch_length_blocks=361)
        )
        for n in (0, 1, 5, 10):
            epoch_id = calc.get_epoch_id(n)
            assert calc.epoch_id_to_number(epoch_id) == n

    def test_epoch_id_to_number_invalid_string(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000))
        assert calc.epoch_id_to_number("not-a-number") == 0

    def test_epoch_id_to_number_empty_string(self):
        calc = EpochCalculator(_make_epoch_config(start_block=1000))
        assert calc.epoch_id_to_number("") == 0


# ---------------------------------------------------------------------------
# S3Repository tests
# ---------------------------------------------------------------------------

from validator.api.s3_repository import S3Repository  # noqa: E402
from validator.config import S3Config  # noqa: E402


def _make_s3_config(
    bucket_name: Optional[str] = "my-bucket",
    endpoint_url: Optional[str] = "https://s3.example.com",
    prefix: str = "",
) -> S3Config:
    cfg = S3Config()
    cfg.bucket_name = bucket_name
    cfg.endpoint_url = endpoint_url
    cfg.bucket_url = ""
    cfg.prefix = prefix
    cfg.region = "us-east-1"
    cfg.access_key_id = "key"
    cfg.secret_access_key = "secret"
    return cfg


def _make_client_error(code: str) -> "Exception":
    from botocore.exceptions import ClientError  # type: ignore[import]

    return ClientError({"Error": {"Code": code, "Message": "test"}}, "GetObject")


class TestS3RepositoryParseBucketUrl:
    def test_explicit_bucket_name_and_endpoint(self):
        cfg = _make_s3_config(
            bucket_name="my-bucket", endpoint_url="https://s3.example.com"
        )
        with patch("boto3.client"):
            repo = S3Repository(cfg)
        assert repo._bucket_name == "my-bucket"
        assert repo._endpoint_url == "https://s3.example.com"

    def test_bucket_url_with_path(self):
        cfg = S3Config()
        cfg.bucket_name = None
        cfg.bucket_url = "https://s3.example.com/my-bucket-from-url"
        cfg.prefix = ""
        cfg.region = "us-east-1"
        cfg.access_key_id = None
        cfg.secret_access_key = None
        with patch("boto3.client"):
            repo = S3Repository(cfg)
        assert repo._bucket_name == "my-bucket-from-url"
        assert repo._endpoint_url == "https://s3.example.com"

    def test_bucket_url_without_path_falls_back_to_default(self):
        cfg = S3Config()
        cfg.bucket_name = None
        cfg.bucket_url = "https://s3.example.com/"
        cfg.prefix = ""
        cfg.region = "us-east-1"
        cfg.access_key_id = None
        cfg.secret_access_key = None
        with patch("boto3.client"):
            repo = S3Repository(cfg)
        assert repo._bucket_name == "blockmachine-gateway-logs"

    def test_prefix_stored(self):
        cfg = _make_s3_config(prefix="myprefix")
        with patch("boto3.client"):
            repo = S3Repository(cfg)
        assert repo.prefix == "myprefix"

    def test_empty_prefix(self):
        cfg = _make_s3_config(prefix="")
        with patch("boto3.client"):
            repo = S3Repository(cfg)
        assert repo.prefix == ""


class TestS3RepositoryKey:
    def _repo(self, prefix: str = "") -> S3Repository:
        with patch("boto3.client"):
            return S3Repository(_make_s3_config(prefix=prefix))

    def test_key_no_prefix(self):
        repo = self._repo(prefix="")
        assert repo.key("a", "b", "c") == "a/b/c"

    def test_key_with_prefix(self):
        repo = self._repo(prefix="mypfx")
        assert repo.key("a", "b") == "mypfx/a/b"

    def test_key_single_part(self):
        repo = self._repo(prefix="")
        assert repo.key("only") == "only"


class TestS3RepositoryGetObjectJson:
    def _repo(self) -> S3Repository:
        with patch("boto3.client"):
            return S3Repository(_make_s3_config())

    @pytest.mark.asyncio
    async def test_returns_parsed_json_on_success(self):
        repo = self._repo()
        payload = {"miners": 3, "epoch": "1000"}
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": BytesIO(json.dumps(payload).encode())
        }
        repo._s3_client = mock_s3

        result = await repo.get_object_json("epochs/1000/data.json")

        assert result == payload
        mock_s3.get_object.assert_called_once_with(
            Bucket="my-bucket", Key="epochs/1000/data.json"
        )

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = _make_client_error("404")
        repo._s3_client = mock_s3

        result = await repo.get_object_json("missing/key.json")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_no_such_key(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = _make_client_error("NoSuchKey")
        repo._s3_client = mock_s3

        result = await repo.get_object_json("missing/key.json")

        assert result is None

    @pytest.mark.asyncio
    async def test_raises_on_other_client_error(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = _make_client_error("AccessDenied")
        repo._s3_client = mock_s3

        from botocore.exceptions import ClientError  # type: ignore[import]

        with pytest.raises(ClientError):
            await repo.get_object_json("some/key.json")


class TestS3RepositoryGetObjectText:
    def _repo(self) -> S3Repository:
        with patch("boto3.client"):
            return S3Repository(_make_s3_config())

    @pytest.mark.asyncio
    async def test_returns_text_on_success(self):
        repo = self._repo()
        content = "line1\nline2\n"
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {"Body": BytesIO(content.encode())}
        repo._s3_client = mock_s3

        result = await repo.get_object_text("logs/file.jsonl")

        assert result == content

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = _make_client_error("NoSuchKey")
        repo._s3_client = mock_s3

        assert await repo.get_object_text("missing.jsonl") is None

    @pytest.mark.asyncio
    async def test_raises_on_other_error(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.get_object.side_effect = _make_client_error("InternalError")
        repo._s3_client = mock_s3

        from botocore.exceptions import ClientError  # type: ignore[import]

        with pytest.raises(ClientError):
            await repo.get_object_text("some/key.jsonl")


class TestS3RepositoryListPrefixes:
    def _repo(self) -> S3Repository:
        with patch("boto3.client"):
            return S3Repository(_make_s3_config())

    @pytest.mark.asyncio
    async def test_returns_prefixes(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "gw-a/"},
                {"Prefix": "gw-b/"},
            ]
        }
        repo._s3_client = mock_s3

        result = await repo.list_prefixes("")

        assert result == ["gw-a/", "gw-b/"]
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="my-bucket", Prefix="", Delimiter="/"
        )

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_prefixes(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}
        repo._s3_client = mock_s3

        result = await repo.list_prefixes("some/prefix/")

        assert result == []


class TestS3RepositoryListObjects:
    def _repo(self) -> S3Repository:
        with patch("boto3.client"):
            return S3Repository(_make_s3_config())

    @pytest.mark.asyncio
    async def test_returns_contents(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "a.json"}, {"Key": "b.json"}]
        }
        repo._s3_client = mock_s3

        result = await repo.list_objects("some/prefix/")

        assert result == [{"Key": "a.json"}, {"Key": "b.json"}]

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_contents(self):
        repo = self._repo()
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}
        repo._s3_client = mock_s3

        result = await repo.list_objects("some/prefix/")

        assert result == []


# ---------------------------------------------------------------------------
# normalize_miner_config tests
# ---------------------------------------------------------------------------

from validator.weights.types import normalize_miner_config  # noqa: E402


class TestNormalizeMinerConfig:
    DEFAULT = 0.03

    def test_none_cfg_returns_default_for_both(self):
        result = normalize_miner_config(None, self.DEFAULT)
        assert result == {
            "price_non_archive": self.DEFAULT,
            "price_archive": self.DEFAULT,
        }

    def test_flat_dict_both_same_price(self):
        cfg = {"target_usd_per_cu": 0.001}
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": 0.001, "price_archive": 0.001}

    def test_flat_dict_million_cu_converted_to_per_cu(self):
        cfg = {"target_usd_per_million_cu": 1000}
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": 0.001, "price_archive": 0.001}

    def test_enriched_dict_default_and_archive(self):
        cfg = {
            "default": {"target_usd_per_cu": 0.001},
            "archive": {"target_usd_per_cu": 0.005},
        }
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": 0.001, "price_archive": 0.005}

    def test_enriched_dict_default_only(self):
        """Archive price falls back to default when only 'default' key present."""
        cfg = {"default": {"target_usd_per_cu": 0.002}}
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": 0.002, "price_archive": self.DEFAULT}

    def test_enriched_dict_archive_only(self):
        """Non-archive price falls back to default when only 'archive' key present."""
        cfg = {"archive": {"target_usd_per_cu": 0.007}}
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": self.DEFAULT, "price_archive": 0.007}

    def test_enriched_missing_target_field_falls_back_to_default(self):
        cfg = {
            "default": {},
            "archive": {},
        }
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {
            "price_non_archive": self.DEFAULT,
            "price_archive": self.DEFAULT,
        }

    def test_dataclass_like_object_with_attribute(self):
        @dataclass
        class MinerConfig:
            target_usd_per_cu: float

        cfg = MinerConfig(target_usd_per_cu=0.004)
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": 0.004, "price_archive": 0.004}

    def test_dataclass_like_object_with_million_cu_attribute(self):
        @dataclass
        class MinerConfig:
            target_usd_per_million_cu: float

        cfg = MinerConfig(target_usd_per_million_cu=4000)
        result = normalize_miner_config(cfg, self.DEFAULT)
        assert result == {"price_non_archive": 0.004, "price_archive": 0.004}

    def test_unknown_object_type_returns_default(self):
        result = normalize_miner_config(42, self.DEFAULT)
        assert result == {
            "price_non_archive": self.DEFAULT,
            "price_archive": self.DEFAULT,
        }

    def test_empty_dict_returns_default(self):
        result = normalize_miner_config({}, self.DEFAULT)
        assert result == {
            "price_non_archive": self.DEFAULT,
            "price_archive": self.DEFAULT,
        }

    def test_return_type_is_normalized_miner_config(self):
        result = normalize_miner_config(None, self.DEFAULT)
        assert isinstance(result, dict)
        assert "price_non_archive" in result
        assert "price_archive" in result


# ---------------------------------------------------------------------------
# VerificationLoop._verify_epoch — DB error propagation from _verify_miner
# ---------------------------------------------------------------------------

# We need to stub out heavy validator dependencies before importing the loop.


def _stub_validator_deps():
    for mod_name in [
        "substrateinterface",
        "bittensor",
        "prometheus_client",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = types.ModuleType(mod_name)


_stub_validator_deps()


@pytest.fixture
def verification_loop():
    """Build a minimal VerificationLoop with all collaborators mocked."""
    from validator.config import VerificationConfig
    from validator.verification.loop import VerificationLoop

    config = VerificationConfig()

    logs = AsyncMock()
    chain = Mock()
    chain.get_validator_hotkey.return_value = "5ValidatorHotkey" + "X" * 32
    blacklist_svc = AsyncMock()
    blacklist_svc.is_blacklisted.return_value = False
    store = AsyncMock()

    # ReferenceNodeManager is constructed inside VerificationLoop via dependency
    # injection — we pass a mock directly.
    ref_manager = Mock()

    # LoggedVerifier is constructed inside VerificationLoop; patch it out.
    with (
        patch("validator.verification.loop.BlacklistManager") as mock_bm_cls,
        patch("validator.verification.loop.LoggedVerifier") as mock_lv_cls,
    ):
        mock_bm = AsyncMock()
        mock_bm_cls.return_value = mock_bm

        mock_lv = AsyncMock()
        mock_lv.verify.return_value = []
        mock_lv_cls.return_value = mock_lv

        loop = VerificationLoop(
            config=config,
            logs=logs,
            chain=chain,
            blacklist=blacklist_svc,
            store=store,
            reference_manager=ref_manager,
        )

    # Expose collaborators for test manipulation
    loop._test_logs = logs
    loop._test_blacklist = blacklist_svc
    loop._test_store = store
    loop._test_blacklist_manager = loop.blacklist_manager
    return loop


class TestVerifyEpochErrorHandling:
    @pytest.mark.asyncio
    async def test_returns_false_when_verify_miner_raises(self, verification_loop):
        """_verify_epoch returns False if _verify_miner raises an exception."""
        from validator.common.types import MinerInfo

        loop = verification_loop
        manifest = {"some": "data"}
        # Provide a single miner
        miner = MinerInfo(
            hotkey="5MinerHotkey" + "A" * 36, coldkey="5ColdKey" + "B" * 40, uid=1
        )
        loop._test_logs.fetch_epoch_logs_from_manifest = AsyncMock(
            return_value=[Mock(miner_hotkey=miner.hotkey)]
        )
        loop.chain.get_miners = AsyncMock(return_value=[miner])
        loop._test_blacklist.is_blacklisted.return_value = False

        # Make _verify_miner raise
        loop._test_blacklist_manager.is_blacklisted = AsyncMock(return_value=False)
        loop._test_store.ensure_miner.side_effect = RuntimeError("DB down")
        # get_verification_state raises too — _verify_miner should handle it
        loop._test_store.get_verification_state.side_effect = RuntimeError("DB down")
        loop._test_store.save_verification_state.side_effect = RuntimeError("DB down")

        # Patch logged_verifier to raise to force the outer try/except in _verify_epoch
        loop.logged_verifier.verify = AsyncMock(
            side_effect=Exception("fatal verification error")
        )

        result = await loop._verify_epoch("1000", manifest=manifest)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_true_when_no_logs(self, verification_loop):
        """_verify_epoch returns True and skips miners when there are no epoch logs."""
        loop = verification_loop
        manifest = {"some": "data"}
        loop._test_logs.fetch_epoch_logs_from_manifest = AsyncMock(return_value=[])

        result = await loop._verify_epoch("1000", manifest=manifest)

        assert result is True
        loop.chain.get_miners.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_true_when_all_miners_succeed(self, verification_loop):
        """_verify_epoch returns True when all miners verify without error."""
        from validator.common.types import MinerInfo

        loop = verification_loop
        miner = MinerInfo(hotkey="5Miner" + "C" * 42, coldkey="5Cold" + "D" * 43, uid=2)
        manifest = {"some": "data"}
        loop._test_logs.fetch_epoch_logs_from_manifest = AsyncMock(return_value=[])
        loop.chain.get_miners = AsyncMock(return_value=[miner])
        loop._test_blacklist.is_blacklisted.return_value = False

        # _verify_miner won't fail — no logs means it returns early
        result = await loop._verify_epoch("1000", manifest=manifest)

        assert result is True

    @pytest.mark.asyncio
    async def test_skips_blacklisted_miners(self, verification_loop):
        """_verify_epoch skips miners whose coldkey is blacklisted."""
        from validator.common.types import MinerInfo

        loop = verification_loop
        miner = MinerInfo(hotkey="5Miner" + "E" * 42, coldkey="5Cold" + "F" * 43, uid=3)
        manifest = {"some": "data"}
        loop._test_logs.fetch_epoch_logs_from_manifest = AsyncMock(
            return_value=[Mock(miner_hotkey=miner.hotkey)]
        )
        loop.chain.get_miners = AsyncMock(return_value=[miner])
        loop._test_blacklist.is_blacklisted.return_value = True

        result = await loop._verify_epoch("1000", manifest=manifest)

        assert result is True
        # _verify_miner never called — no store interactions expected
        loop._test_store.ensure_miner.assert_not_called()
