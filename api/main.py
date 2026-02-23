"""
FastAPI application entry point.
Lifespan manages connection pool + Redis init/close.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2

from api.database import init_pool, close_pool
from api.cache import init_redis, close_redis
from api.middleware import RequestLoggingMiddleware, RateLimitMiddleware
from api.routers import auth, pipeline, carriers, delays, routes, weather, airports, chat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_pool()
    try:
        init_redis()
    except Exception:
        logging.getLogger("flight-api").warning("Redis unavailable — caching disabled")
    yield
    # Shutdown
    close_redis()
    close_pool()


app = FastAPI(
    title="Flight Data Pipeline API",
    description="Backend service for 6.4M US domestic flights + 3M weather observations",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom middleware
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(RateLimitMiddleware)

# Routers
app.include_router(auth.router, prefix="/api/v1/auth", tags=["Auth"])
app.include_router(pipeline.router, prefix="/api/v1/pipeline", tags=["Pipeline"])
app.include_router(carriers.router, prefix="/api/v1/carriers", tags=["Carriers"])
app.include_router(delays.router, prefix="/api/v1/delays", tags=["Delays"])
app.include_router(routes.router, prefix="/api/v1/routes", tags=["Routes"])
app.include_router(weather.router, prefix="/api/v1/weather", tags=["Weather Impact"])
app.include_router(airports.router, prefix="/api/v1/airports", tags=["Airports"])
app.include_router(chat.router, prefix="/api/v1/chat", tags=["Chat"])


# Global error handlers
@app.exception_handler(psycopg2.OperationalError)
async def db_error_handler(request: Request, exc: psycopg2.OperationalError):
    return JSONResponse(
        status_code=503,
        content={
            "error": "Database unavailable",
            "detail": str(exc),
            "status_code": 503,
        },
    )


@app.exception_handler(psycopg2.errors.QueryCanceled)
async def query_timeout_handler(request: Request, exc):
    return JSONResponse(
        status_code=504,
        content={
            "error": "Query timeout",
            "detail": "Query exceeded time limit",
            "status_code": 504,
        },
    )
