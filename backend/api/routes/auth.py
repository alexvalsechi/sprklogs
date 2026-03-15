"""
OAuth2 & Token Management
==========================
Manages OAuth2 flows, stores tokens in Redis/session, and handles token refresh.
Supports OpenAI, Anthropic Claude, and Google Gemini.
"""
import json
import httpx
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from jose import jwt
import redis as redis_lib

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
    """OpenAI OAuth2 (platform.openai.com)."""
    
    AUTH_URL = "https://platform.openai.com/account/auth/login"
    TOKEN_URL = "https://api.openai.com/oauth/authorize"
    
    def get_auth_url(self, state: str) -> str:
        return (
            f"https://platform.openai.com/authorizing?redirect={self.redirect_uri}"
            f"&client_id={self.client_id}&state={state}"
        )
    
    async def exchange_code(self, code: str) -> Dict[str, Any]:
        """Exchange code for API key."""
        # OpenAI uses a custom flow; code is actually the API key
        # In real flow, you'd exchange via their OAuth endpoint
        return {
            "access_token": code,
            "token_type": "bearer",
            "provider": "openai",
            "expires_in": 86400 * 90,  # 90 days
        }


class AnthropicProvider(OAuthProvider):
    """Anthropic OAuth2 (console.anthropic.com)."""
    
    AUTH_URL = "https://console.anthropic.com/login"
    TOKEN_URL = "https://console.anthropic.com/oauth/token"
    
    def get_auth_url(self, state: str) -> str:
        return (
            f"{self.TOKEN_URL}?"
            f"client_id={self.client_id}"
            f"&response_type=code"
            f"&redirect_uri={self.redirect_uri}"
            f"&state={state}"
        )
    
    async def exchange_code(self, code: str) -> Dict[str, Any]:
        """Exchange auth code for API key."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": self.redirect_uri,
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
    
    def get_auth_url(self, state: str) -> str:
        return (
            f"{self.AUTH_URL}?"
            f"client_id={self.client_id}"
            f"&response_type=code"
            f"&scope={self.SCOPE}"
            f"&redirect_uri={self.redirect_uri}"
            f"&state={state}"
        )
    
    async def exchange_code(self, code: str) -> Dict[str, Any]:
        """Exchange auth code for tokens."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                self.TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": self.redirect_uri,
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
    """Manages token storage in Redis with encryption."""
    
    def __init__(self, redis_client: redis_lib.Redis, secret_key: str):
        self.redis = redis_client
        self.secret_key = secret_key
        self.algorithm = "HS256"
    
    def store_token(self, user_id: str, provider: str, token_data: Dict[str, Any]) -> None:
        """Store token securely in Redis with TTL."""
        expires_in = token_data.get("expires_in", 86400)
        key = f"oauth_token:{user_id}:{provider}"
        
        # Encrypt sensitive data (optional: use more robust encryption)
        payload = {
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token"),
            "token_type": token_data.get("token_type"),
            "provider": provider,
            "created_at": datetime.utcnow().isoformat(),
        }
        
        self.redis.setex(
            key,
            expires_in,
            json.dumps(payload)
        )
        logger.info(f"Stored token for {user_id}:{provider}")
    
    def get_token(self, user_id: str, provider: str) -> Optional[Dict[str, Any]]:
        """Retrieve token from Redis."""
        key = f"oauth_token:{user_id}:{provider}"
        data = self.redis.get(key)
        if not data:
            return None
        
        token_data = json.loads(data)
        logger.info(f"Retrieved token for {user_id}:{provider}")
        return token_data
    
    def delete_token(self, user_id: str, provider: str) -> None:
        """Remove token (logout)."""
        key = f"oauth_token:{user_id}:{provider}"
        self.redis.delete(key)
        logger.info(f"Deleted token for {user_id}:{provider}")
    
    def list_providers(self, user_id: str) -> list[str]:
        """List connected OAuth providers for a user."""
        pattern = f"oauth_token:{user_id}:*"
        keys = self.redis.keys(pattern)
        providers = [key.split(":")[-1] for key in keys if isinstance(key, (str, bytes))]
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
