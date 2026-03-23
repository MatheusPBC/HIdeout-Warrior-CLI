from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import requests

DEFAULT_POE_TOKEN_URL = "https://www.pathofexile.com/oauth/token"
DEFAULT_SERVICE_SCOPE = "service:psapi"
DEFAULT_OAUTH_USER_AGENT = (
    "OAuth hideout-warrior-cli/1.0.0 (contact: hideout-warrior-cli@local)"
)


@dataclass(frozen=True)
class OAuthAccessToken:
    access_token: str
    token_type: str = "bearer"
    scope: Optional[str] = None
    source: str = "direct_token"
    expires_in: Optional[int] = None
    username: Optional[str] = None
    subject: Optional[str] = None


def _clean_optional_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _resolve_client_id(explicit_client_id: Optional[str]) -> Optional[str]:
    return (
        _clean_optional_str(explicit_client_id)
        or _clean_optional_str(os.getenv("POE_OAUTH_CLIENT_ID"))
        or _clean_optional_str(os.getenv("POE_CLIENT_ID"))
    )


def _resolve_client_secret(explicit_client_secret: Optional[str]) -> Optional[str]:
    return (
        _clean_optional_str(explicit_client_secret)
        or _clean_optional_str(os.getenv("POE_OAUTH_CLIENT_SECRET"))
        or _clean_optional_str(os.getenv("POE_CLIENT_SECRET"))
    )


def request_client_credentials_token(
    client_id: str,
    client_secret: str,
    scope: str = DEFAULT_SERVICE_SCOPE,
    token_url: str = DEFAULT_POE_TOKEN_URL,
    user_agent: Optional[str] = None,
    timeout_seconds: float = 20.0,
    session: Optional[Any] = None,
) -> OAuthAccessToken:
    http_client = session or requests
    headers = {"Accept": "application/json"}
    headers["User-Agent"] = user_agent or DEFAULT_OAUTH_USER_AGENT

    response = http_client.post(
        token_url,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": scope,
        },
        headers=headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = _clean_optional_str(payload.get("access_token"))
    token_type = _clean_optional_str(payload.get("token_type")) or "bearer"
    if not access_token:
        raise RuntimeError("OAuth token endpoint respondeu sem access_token")

    expires_in_raw = payload.get("expires_in")
    expires_in = (
        int(expires_in_raw) if isinstance(expires_in_raw, (int, float)) else None
    )
    return OAuthAccessToken(
        access_token=access_token,
        token_type=token_type,
        scope=_clean_optional_str(payload.get("scope")) or scope,
        source="client_credentials",
        expires_in=expires_in,
        username=_clean_optional_str(payload.get("username")),
        subject=_clean_optional_str(payload.get("sub")),
    )


def resolve_service_oauth_token(
    access_token: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    scope: str = DEFAULT_SERVICE_SCOPE,
    token_url: str = DEFAULT_POE_TOKEN_URL,
    user_agent: Optional[str] = None,
    timeout_seconds: float = 20.0,
    session: Optional[Any] = None,
) -> Optional[OAuthAccessToken]:
    direct_token = _clean_optional_str(access_token) or _clean_optional_str(
        os.getenv("POE_OAUTH_TOKEN")
    )
    if direct_token:
        return OAuthAccessToken(
            access_token=direct_token,
            scope=scope,
            source="direct_token",
        )

    resolved_client_id = _resolve_client_id(client_id)
    resolved_client_secret = _resolve_client_secret(client_secret)
    if bool(resolved_client_id) != bool(resolved_client_secret):
        raise ValueError(
            "OAuth client incompleto: informe client_id e client_secret juntos"
        )
    if not resolved_client_id or not resolved_client_secret:
        return None

    return request_client_credentials_token(
        client_id=resolved_client_id,
        client_secret=resolved_client_secret,
        scope=scope,
        token_url=token_url,
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        session=session,
    )
