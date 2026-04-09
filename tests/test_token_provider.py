"""Tests for validator TokenProvider and gateway 401 handling."""

import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

from validator.auth import TokenProvider
from validator.verification.reference import GatewayReferenceClient

_DEFAULTS = dict(
    auth_url="https://auth.example.com",
    client_id="test-client",
    netuid=19,
    hotkey_ss58="5FakeAddress",
    sign_fn=lambda m: b"\xde\xad",
)


def _make_provider(**overrides) -> TokenProvider:
    kw = {**_DEFAULTS, **overrides}
    return TokenProvider(**kw)


def _challenge_mock_post(url, **kwargs):
    """Mock HTTP post that handles challenge + verify endpoints."""
    if "/v1/auth/challenge" in url:
        return httpx.Response(
            200,
            json={"nonce": "nonce-abc", "expires_in": 300},
            request=httpx.Request("POST", url),
        )
    if "/v1/auth/verify" in url:
        return httpx.Response(
            200,
            json={"access_token": "new-token", "expires_in": 3600},
            request=httpx.Request("POST", url),
        )
    return httpx.Response(404, request=httpx.Request("POST", url))


@pytest.fixture
def provider():
    """TokenProvider with pre-set tokens for refresh tests."""
    tp = _make_provider()
    tp.access_token = "old-access"
    tp.refresh_token = "old-refresh"
    return tp


class TestTokenProviderRefresh:
    """Test refresh coordination under concurrency."""

    @pytest.mark.asyncio
    async def test_concurrent_refresh_deduplicates(self, provider):
        """Two concurrent refresh() calls produce only one HTTP request."""
        call_count = 0
        gate = asyncio.Event()

        async def mock_post(url, **kwargs):
            nonlocal call_count
            call_count += 1
            await gate.wait()
            return httpx.Response(
                200,
                json={
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 3600,
                },
                request=httpx.Request("POST", url),
            )

        provider._http = AsyncMock()
        provider._http.post = mock_post

        task1 = asyncio.create_task(provider.refresh())
        task2 = asyncio.create_task(provider.refresh())

        await asyncio.sleep(0.01)
        gate.set()

        r1, r2 = await asyncio.gather(task1, task2)

        assert r1 is True
        assert r2 is True
        assert call_count == 1, f"Expected 1 HTTP request, got {call_count}"
        assert provider.access_token == "new-access"
        assert provider.refresh_token == "new-refresh"

    @pytest.mark.asyncio
    async def test_refresh_failure_sets_backoff(self, provider):
        """A failed refresh sets backoff so subsequent calls are skipped."""

        async def mock_post(url, **kwargs):
            raise httpx.HTTPStatusError(
                "Unauthorized",
                request=httpx.Request("POST", url),
                response=httpx.Response(401),
            )

        provider._http = AsyncMock()
        provider._http.post = mock_post

        result = await provider.refresh()

        assert result is False
        assert provider._backoff_until > 0

        result2 = await provider.refresh()
        assert result2 is False

    @pytest.mark.asyncio
    async def test_refresh_no_refresh_token(self, provider):
        """Refresh returns False when no refresh token is available."""
        provider.refresh_token = ""
        assert await provider.refresh() is False

    @pytest.mark.asyncio
    async def test_refresh_rotates_refresh_token(self, provider):
        """When server returns a new refresh token, it is stored."""

        async def mock_post(url, **kwargs):
            return httpx.Response(
                200,
                json={
                    "access_token": "new-access",
                    "refresh_token": "rotated-refresh",
                    "expires_in": 3600,
                },
                request=httpx.Request("POST", url),
            )

        provider._http = AsyncMock()
        provider._http.post = mock_post

        result = await provider.refresh()

        assert result is True
        assert provider.refresh_token == "rotated-refresh"


class TestTokenProviderChallengeFlow:
    """Test challenge flow authentication."""

    @pytest.mark.asyncio
    async def test_challenge_flow_success(self):
        """Challenge flow requests nonce, signs, and stores tokens."""
        call_log = []

        async def mock_post(url, **kwargs):
            call_log.append(url)
            if "/v1/auth/challenge" in url:
                return httpx.Response(
                    200,
                    json={"nonce": "test-nonce-123", "expires_in": 300},
                    request=httpx.Request("POST", url),
                )
            if "/v1/auth/verify" in url:
                return httpx.Response(
                    200,
                    json={
                        "access_token": "challenge-access",
                        "expires_in": 3600,
                        "scope": "subnet:417:validator",
                    },
                    request=httpx.Request("POST", url),
                )
            return httpx.Response(404, request=httpx.Request("POST", url))

        tp = _make_provider(netuid=417, sign_fn=lambda m: b"\xde\xad\xbe\xef")
        tp._http = AsyncMock()
        tp._http.post = mock_post

        await tp.ensure_authenticated()

        assert tp.access_token == "challenge-access"
        assert len(call_log) == 2
        assert "/v1/auth/challenge" in call_log[0]
        assert "/v1/auth/verify" in call_log[1]

    @pytest.mark.asyncio
    async def test_challenge_flow_skipped_when_tokens_exist(self):
        """ensure_authenticated is a no-op when tokens already loaded."""
        tp = _make_provider()
        tp.access_token = "existing"
        tp._http = AsyncMock()

        await tp.ensure_authenticated()
        tp._http.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_challenge_flow_verify_failure_raises(self):
        """Challenge flow raises on verify endpoint failure."""

        async def mock_post(url, **kwargs):
            if "/v1/auth/challenge" in url:
                return httpx.Response(
                    200,
                    json={"nonce": "test-nonce", "expires_in": 300},
                    request=httpx.Request("POST", url),
                )
            return httpx.Response(
                401,
                json={"error": "invalid_signature"},
                request=httpx.Request("POST", url),
            )

        tp = _make_provider()
        tp._http = AsyncMock()
        tp._http.post = mock_post

        with pytest.raises(httpx.HTTPStatusError):
            await tp.ensure_authenticated()


class TestTokenProviderReauthenticate:
    """Test reauthenticate() — lock-protected challenge flow with backoff."""

    @pytest.mark.asyncio
    async def test_reauthenticate_success(self):
        tp = _make_provider()
        tp._http = AsyncMock()
        tp._http.post = AsyncMock(side_effect=_challenge_mock_post)

        result = await tp.reauthenticate()

        assert result is True
        assert tp.access_token == "new-token"

    @pytest.mark.asyncio
    async def test_reauthenticate_failure_sets_backoff(self):
        tp = _make_provider()
        tp._http = AsyncMock()

        async def fail_challenge(url, **kwargs):
            if "/v1/auth/challenge" in url:
                return httpx.Response(
                    500,
                    json={"error": "server_error"},
                    request=httpx.Request("POST", url),
                )
            return httpx.Response(404, request=httpx.Request("POST", url))

        tp._http.post = fail_challenge

        result = await tp.reauthenticate()
        assert result is False
        assert tp._backoff_until > 0

        result2 = await tp.reauthenticate()
        assert result2 is False

    @pytest.mark.asyncio
    async def test_concurrent_reauthenticate_deduplicates(self):
        """Two concurrent reauthenticate() calls produce only one challenge flow."""
        call_count = 0
        gate = asyncio.Event()

        async def slow_mock_post(url, **kwargs):
            nonlocal call_count
            if "/v1/auth/challenge" in url:
                call_count += 1
                await gate.wait()
            return _challenge_mock_post(url, **kwargs)

        tp = _make_provider()
        tp._http = AsyncMock()
        tp._http.post = slow_mock_post

        t1 = asyncio.create_task(tp.reauthenticate())
        t2 = asyncio.create_task(tp.reauthenticate())
        await asyncio.sleep(0.01)
        gate.set()
        r1, r2 = await asyncio.gather(t1, t2)

        assert r1 is True
        assert r2 is True
        assert call_count == 1, f"Expected 1 challenge request, got {call_count}"


class TestGatewayClient401Fallback:
    """Test that GatewayReferenceClient falls back to reauthenticate on 401."""

    @pytest.mark.asyncio
    async def test_401_refresh_fails_challenge_succeeds(self):
        """On 401, if refresh fails, reauthenticate is called and request retried."""
        tp = _make_provider()
        tp.access_token = "expired-token"
        tp.refresh_token = ""

        client = GatewayReferenceClient(
            gateway_url="https://gw.example.com",
            chain="eth",
            token_provider=tp,
        )

        async def mock_rpc_post(url, **kwargs):
            if "/rpc/" in url:
                token = kwargs.get("headers", {}).get("Authorization", "")
                if "new-token" in token:
                    return httpx.Response(
                        200,
                        json={"jsonrpc": "2.0", "id": 1, "result": "0x1"},
                        request=httpx.Request("POST", url),
                    )
                return httpx.Response(
                    401,
                    json={"error": "unauthorized"},
                    request=httpx.Request("POST", url),
                )
            return _challenge_mock_post(url, **kwargs)

        client._client = AsyncMock()
        client._client.post = mock_rpc_post
        tp._http = AsyncMock()
        tp._http.post = AsyncMock(side_effect=_challenge_mock_post)

        result = await client.query("eth_blockNumber", [])

        assert result == "0x1"
        assert tp.access_token == "new-token"

    @pytest.mark.asyncio
    async def test_401_both_refresh_and_challenge_fail(self):
        """On 401, if both refresh and reauthenticate fail, request raises."""
        tp = _make_provider()
        tp.access_token = "expired-token"
        tp.refresh_token = ""

        client = GatewayReferenceClient(
            gateway_url="https://gw.example.com",
            chain="eth",
            token_provider=tp,
        )

        async def always_401(url, **kwargs):
            return httpx.Response(
                401,
                json={"error": "unauthorized"},
                request=httpx.Request("POST", url),
            )

        async def fail_challenge(url, **kwargs):
            return httpx.Response(
                500,
                json={"error": "server_error"},
                request=httpx.Request("POST", url),
            )

        client._client = AsyncMock()
        client._client.post = always_401
        tp._http = AsyncMock()
        tp._http.post = fail_challenge

        with pytest.raises(httpx.HTTPStatusError):
            await client.query("eth_blockNumber", [])
