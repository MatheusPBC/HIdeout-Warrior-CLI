from core.poe_oauth import (
    DEFAULT_OAUTH_USER_AGENT,
    DEFAULT_SERVICE_SCOPE,
    OAuthAccessToken,
    request_client_credentials_token,
    resolve_service_oauth_token,
)


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _HttpClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def post(self, url, data, headers, timeout):
        self.calls.append(
            {
                "url": url,
                "data": data,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return _Response(self.payload)


def test_resolve_service_oauth_token_prefers_direct_token_without_http(monkeypatch):
    monkeypatch.setenv("POE_OAUTH_TOKEN", "env-token")

    token = resolve_service_oauth_token(access_token="cli-token")

    assert token == OAuthAccessToken(
        access_token="cli-token",
        scope=DEFAULT_SERVICE_SCOPE,
        source="direct_token",
    )


def test_request_client_credentials_token_posts_expected_payload():
    http_client = _HttpClient(
        {
            "access_token": "svc-token",
            "token_type": "bearer",
            "scope": "service:psapi",
            "expires_in": None,
            "username": "tester",
            "sub": "sub-123",
        }
    )

    token = request_client_credentials_token(
        client_id="client-id",
        client_secret="client-secret",
        session=http_client,
        user_agent="hideout-warrior-tests",
    )

    assert token.access_token == "svc-token"
    assert token.scope == "service:psapi"
    assert token.source == "client_credentials"
    assert http_client.calls[0]["data"] == {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "grant_type": "client_credentials",
        "scope": "service:psapi",
    }
    assert http_client.calls[0]["headers"]["User-Agent"] == "hideout-warrior-tests"


def test_request_client_credentials_token_uses_default_user_agent():
    http_client = _HttpClient(
        {
            "access_token": "svc-token",
            "token_type": "bearer",
            "scope": "service:psapi",
        }
    )

    request_client_credentials_token(
        client_id="client-id",
        client_secret="client-secret",
        session=http_client,
    )

    assert http_client.calls[0]["headers"]["User-Agent"] == DEFAULT_OAUTH_USER_AGENT


def test_resolve_service_oauth_token_uses_env_client_credentials(monkeypatch):
    monkeypatch.delenv("POE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("POE_OAUTH_CLIENT_ID", "env-client")
    monkeypatch.setenv("POE_OAUTH_CLIENT_SECRET", "env-secret")
    http_client = _HttpClient(
        {
            "access_token": "env-generated-token",
            "token_type": "bearer",
            "scope": "service:psapi",
        }
    )

    token = resolve_service_oauth_token(session=http_client)

    assert token is not None
    assert token.access_token == "env-generated-token"
    assert token.source == "client_credentials"


def test_resolve_service_oauth_token_requires_complete_client_credentials(monkeypatch):
    monkeypatch.delenv("POE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("POE_OAUTH_CLIENT_ID", raising=False)
    monkeypatch.delenv("POE_OAUTH_CLIENT_SECRET", raising=False)

    try:
        resolve_service_oauth_token(client_id="client-only")
    except ValueError as exc:
        assert "client_id e client_secret" in str(exc)
    else:
        raise AssertionError("Era esperado ValueError para credenciais incompletas")
