"""
Spark Log Analyzer — FastAPI entrypoint.
Applies: Dependency Injection, clean controller delegation.
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from api.routes import router
from utils.config import get_settings
from utils.logging_config import setup_logging

settings = get_settings()


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

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

# Serve the SPA frontend
app.mount("/", StaticFiles(directory="../frontend", html=True), name="frontend")
