"""SprkLogs FastAPI entrypoint."""
import argparse
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import uvicorn

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from backend.api.routes.core import router
    from backend.utils.config import get_settings
    from backend.utils.logging_config import setup_logging
else:
    from .api.routes.core import router
    from .utils.config import get_settings
    from .utils.logging_config import setup_logging

settings = get_settings()

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logging.getLogger(__name__).info("SprkLogs starting up…")
    yield
    logging.getLogger(__name__).info("Shutting down…")


app = FastAPI(
    title="SprkLogs",
    description="Reduce and analyze Apache Spark event logs with AI-powered diagnostics.",
    version="2.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


ROOT_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = ROOT_DIR / "apps" / "web"
if not FRONTEND_DIR.exists():
    FRONTEND_DIR = ROOT_DIR / "frontend"
HAS_FRONTEND = FRONTEND_DIR.exists() and (FRONTEND_DIR / "index.html").exists()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


@app.get("/", include_in_schema=False)
def landing_page():
    if HAS_FRONTEND:
        return FileResponse(FRONTEND_DIR / "index.html")
    return {"status": "ok", "service": "sprklogs-backend"}


if HAS_FRONTEND:
    app.mount(
        "/",
        StaticFiles(directory=str(FRONTEND_DIR), html=True),
        name="frontend",
    )

    # Add cache headers via middleware (StaticFiles doesn't support headers param
    # in all Starlette versions). Only apply to static frontend files, not API.
    from fastapi import Request
    from starlette.middleware.base import BaseHTTPMiddleware

    class CacheControlMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            response = await call_next(request)
            path = request.url.path
            # Only cache static frontend files, not API routes
            if not path.startswith("/api") and not path.startswith("/docs") and not path.startswith("/openapi"):
                response.headers["Cache-Control"] = "public, max-age=3600, stale-while-revalidate=86400"
            return response

    app.add_middleware(CacheControlMiddleware)


if __name__ == "__main__":
    args = parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port)


