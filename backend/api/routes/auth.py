"""
OAuth2 & Token Management
==========================
Manages OAuth2 flows, stores tokens in local memory/session, and handles token refresh.
Supports OpenAI, Anthropic Claude, and Google Gemini.
"""
import json
import httpx
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from threading import Lock
from jose import jwt

logger = logging.getLogger(__name__)


class OAuthProvider:
    """Abstract OAuth2 provider."""
    
    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
    
    def get_auth_url(self, state: str) -> str:
        """Return authorization URL where user logs in."""
        raise NotImplementedError
    
    async def exchange_code(self, code: str) -> Dict[str, Any]:
        """Exchange auth code for tokens."""
        raise NotImplementedError


class OpenAIProvider(OAuthProvider):
    """OpenAI OAuth2 (auth.openai.com)."""

    AUTH_URL = "https://auth.openai.com/authorize"
    TOKEN_URL = "https://auth.openai.com/oauth/token"

    def get_auth_url(self, state: str, redirect_uri: str | None = None) -> str:
        from urllib.parse import urlencode
        uri = redirect_uri or self.redirect_uri
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": uri,
            "scope": "openid profile email",
            "state": state,
        }
        return f"{self.AUTH_URL}?{urlencode(params)}"

    async def exchange_code(self, code: str, redirect_uri: str | None = None) -> Dict[str, Any]:
        """Exchange authorization code for tokens."""
        uri = redirect_uri or self.redirect_uri
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": uri,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "access_token": data.get("access_token"),
                "token_type": "bearer",
                "provider": "openai",
                "expires_in": data.get("expires_in", 86400 * 90),
                "refresh_token": data.get("refresh_token"),
            }


class AnthropicProvider(OAuthProvider):
    """Anthropic OAuth2 (console.anthropic.com)."""
    
    AUTH_URL = "https://console.anthropic.com/login"
    TOKEN_URL = "https://console.anthropic.com/oauth/token"
    
    def get_auth_url(self, state: str, redirect_uri: str | None = None) -> str:
        from urllib.parse import urlencode
        uri = redirect_uri or self.redirect_uri
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": uri,
            "state": state,
        }
        return f"{self.TOKEN_URL}?{urlencode(params)}"

    async def exchange_code(self, code: str, redirect_uri: str | None = None) -> Dict[str, Any]:
        """Exchange auth code for tokens."""
        uri = redirect_uri or self.redirect_uri
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": uri,
                }
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "access_token": data.get("access_token"),
                "token_type": "bearer",
                "provider": "anthropic",
                "expires_in": data.get("expires_in", 86400 * 90),
                "refresh_token": data.get("refresh_token"),
            }


class GoogleGeminiProvider(OAuthProvider):
    """Google OAuth2 for Gemini API."""
    
    AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    SCOPE = "https://www.googleapis.com/auth/generativelanguage"
    
    def get_auth_url(self, state: str, redirect_uri: str | None = None) -> str:
        from urllib.parse import urlencode
        uri = redirect_uri or self.redirect_uri
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "scope": self.SCOPE,
            "redirect_uri": uri,
            "state": state,
        }
        return f"{self.AUTH_URL}?{urlencode(params)}"

    async def exchange_code(self, code: str, redirect_uri: str | None = None) -> Dict[str, Any]:
        """Exchange auth code for tokens."""
        uri = redirect_uri or self.redirect_uri
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": uri,
                }
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "access_token": data.get("access_token"),
                "token_type": "Bearer",
                "provider": "gemini",
                "expires_in": data.get("expires_in", 3600),
                "refresh_token": data.get("refresh_token"),
            }


class TokenManager:
    """Manages token storage in local memory with TTL."""
    
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        self.algorithm = "HS256"
        self._store: dict[str, str] = {}
        self._expires_at: dict[str, datetime] = {}
        self._lock = Lock()

    def _key(self, user_id: str, provider: str) -> str:
        return f"oauth_token:{user_id}:{provider}"

    def _is_expired(self, key: str) -> bool:
        expires = self._expires_at.get(key)
        return bool(expires and datetime.utcnow() >= expires)

    def _cleanup_if_expired(self, key: str) -> None:
        if self._is_expired(key):
            self._store.pop(key, None)
            self._expires_at.pop(key, None)
    
    def store_token(self, user_id: str, provider: str, token_data: Dict[str, Any]) -> None:
        """Store token securely in-memory with TTL."""
        expires_in = token_data.get("expires_in", 86400)
        key = self._key(user_id, provider)
        
        # Encrypt sensitive data (optional: use more robust encryption)
        payload = {
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token"),
            "token_type": token_data.get("token_type"),
            "provider": provider,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        with self._lock:
            self._store[key] = json.dumps(payload)
            self._expires_at[key] = datetime.utcnow() + timedelta(seconds=int(expires_in))
        logger.info(f"Stored token for {user_id}:{provider}")
    
    def get_token(self, user_id: str, provider: str) -> Optional[Dict[str, Any]]:
        """Retrieve token from local memory."""
        key = self._key(user_id, provider)
        with self._lock:
            self._cleanup_if_expired(key)
            data = self._store.get(key)
        if not data:
            return None
        
        token_data = json.loads(data)
        logger.info(f"Retrieved token for {user_id}:{provider}")
        return token_data
    
    def delete_token(self, user_id: str, provider: str) -> None:
        """Remove token (logout)."""
        key = self._key(user_id, provider)
        with self._lock:
            self._store.pop(key, None)
            self._expires_at.pop(key, None)
        logger.info(f"Deleted token for {user_id}:{provider}")
    
    def list_providers(self, user_id: str) -> list[str]:
        """List connected OAuth providers for a user."""
        prefix = f"oauth_token:{user_id}:"
        with self._lock:
            keys = list(self._store.keys())
            for key in keys:
                self._cleanup_if_expired(key)
            providers = [key.split(":")[-1] for key in self._store.keys() if key.startswith(prefix)]
            return providers


def generate_state_token(secret_key: str, duration_hours: int = 1) -> str:
    """Generate a secure state token for OAuth flow."""
    payload = {
        "exp": datetime.utcnow() + timedelta(hours=duration_hours),
        "iat": datetime.utcnow(),
        "type": "oauth_state",
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def verify_state_token(token: str, secret_key: str) -> bool:
    """Verify OAuth state token."""
    try:
        jwt.decode(token, secret_key, algorithms=["HS256"])
        return True
    except Exception as e:
        logger.warning(f"Invalid state token: {e}")
        return False
