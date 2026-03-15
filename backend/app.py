"""Spark Log Analyzer FastAPI entrypoint."""
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
    from backend.api.routes.oauth_routes import router as oauth_router
    from backend.utils.config import get_settings
    from backend.utils.logging_config import setup_logging
else:
    from .api.routes.core import router
    from .api.routes.oauth_routes import router as oauth_router
    from .utils.config import get_settings
    from .utils.logging_config import setup_logging

settings = get_settings()

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logging.getLogger(__name__).info("Spark Log Analyzer starting up…")
    yield
    logging.getLogger(__name__).info("Shutting down…")


app = FastAPI(
    title="Spark Log Analyzer",
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
app.include_router(oauth_router, prefix="/api")


ROOT_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = ROOT_DIR / "apps" / "web"
if not FRONTEND_DIR.exists():
    FRONTEND_DIR = ROOT_DIR / "frontend"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


@app.get("/", include_in_schema=False)
def landing_page():
    return FileResponse(FRONTEND_DIR / "sprklogs-landing.html")


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


if __name__ == "__main__":
    args = parse_args()
    uvicorn.run(app, host="127.0.0.1", port=args.port)


