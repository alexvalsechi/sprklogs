"""
OAuth2 Routes
=============
Handles login, callback, and logout for OAuth2 providers.
"""
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse, JSONResponse
import logging
import uuid

from backend.api.routes.auth import (
    OpenAIProvider, AnthropicProvider, GoogleGeminiProvider,
    TokenManager, generate_state_token, verify_state_token
)
from backend.utils.config import get_settings

router = APIRouter(prefix="/auth")
logger = logging.getLogger(__name__)
settings = get_settings()

# OAuth providers
providers = {
    "openai": OpenAIProvider(
        client_id=settings.openai_oauth_client_id,
        client_secret=settings.openai_oauth_client_secret,
        redirect_uri=f"{settings.frontend_url}/auth/callback/openai"
    ) if settings.openai_oauth_client_id else None,
    
    "anthropic": AnthropicProvider(
        client_id=settings.anthropic_oauth_client_id,
        client_secret=settings.anthropic_oauth_client_secret,
        redirect_uri=f"{settings.frontend_url}/auth/callback/anthropic"
    ) if settings.anthropic_oauth_client_id else None,
    
    "gemini": GoogleGeminiProvider(
        client_id=settings.google_oauth_client_id,
        client_secret=settings.google_oauth_client_secret,
        redirect_uri=f"{settings.frontend_url}/auth/callback/gemini"
    ) if settings.google_oauth_client_id else None,
}

token_manager = TokenManager(settings.secret_key)


@router.get("/login/{provider}")
async def login(provider: str, request: Request):
    """Redirect user to OAuth provider's login page."""
    if provider not in providers or not providers[provider]:
        raise HTTPException(status_code=400, detail=f"Provider '{provider}' not configured")

    # Generate state token
    state = generate_state_token(settings.secret_key)

    # Derive redirect_uri from the actual request host so the callback URL
    # always matches the running backend port, regardless of settings.frontend_url.
    base = str(request.base_url).rstrip("/")
    redirect_uri = f"{base}/auth/callback/{provider}"

    response = RedirectResponse(url=providers[provider].get_auth_url(state, redirect_uri))
    response.set_cookie("oauth_state", state, max_age=3600, httponly=True)

    logger.info(f"Initiating OAuth login for provider: {provider}, redirect_uri: {redirect_uri}")
    return response


@router.get("/callback/{provider}")
async def callback(provider: str, code: str = Query(...), state: str = Query(...), request: Request = None):
    """Handle OAuth callback."""
    if provider not in providers or not providers[provider]:
        raise HTTPException(status_code=400, detail=f"Provider '{provider}' not configured")
    
    # Verify state token
    stored_state = request.cookies.get("oauth_state") if request else None
    if not stored_state or not verify_state_token(stored_state, settings.secret_key):
        raise HTTPException(status_code=400, detail="Invalid OAuth state")
    
    # Exchange code for token
    try:
        base = str(request.base_url).rstrip("/")
        redirect_uri = f"{base}/auth/callback/{provider}"
        token_data = await providers[provider].exchange_code(code, redirect_uri)

        user_id = str(uuid.uuid4())

        if token_manager:
            token_manager.store_token(user_id, provider, token_data)
            logger.info(f"Successfully authenticated user for {provider}")

        # Redirect back to the Electron renderer root with auth params so the
        # OAuth BrowserWindow navigation listener can capture the result.
        response = RedirectResponse(
            url=f"{base}/?oauth_user={user_id}&provider={provider}"
        )
        response.delete_cookie("oauth_state")
        return response

    except Exception as e:
        logger.error(f"OAuth callback failed for {provider}: {e}")
        raise HTTPException(status_code=500, detail="Authentication failed")


@router.post("/logout/{provider}")
async def logout(provider: str, user_id: str = Query(...)):
    """Logout and delete token."""
    token_manager.delete_token(user_id, provider)
    logger.info(f"User {user_id} logged out from {provider}")
    return {"message": "Logged out successfully"}


@router.get("/providers/{user_id}")
async def list_connected_providers(user_id: str):
    """List OAuth providers connected to this user."""
    connected = token_manager.list_providers(user_id)
    return {
        "user_id": user_id,
        "providers": connected,
        "available": list(providers.keys()),
    }


@router.get("/status/{user_id}/{provider}")
async def check_token_status(user_id: str, provider: str):
    """Check if token is valid/connected."""
    token = token_manager.get_token(user_id, provider)
    if not token:
        return {"connected": False, "provider": provider}
    
    return {
        "connected": True,
        "provider": provider,
        "token_type": token.get("token_type"),
        "created_at": token.get("created_at"),
    }
