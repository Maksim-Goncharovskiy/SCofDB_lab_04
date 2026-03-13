"""Main FastAPI application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.api.payment_routes import router as payment_router
from app.middleware.idempotency_middleware import IdempotencyMiddleware

app = FastAPI(
    title="Marketplace API",
    description="DDD-based marketplace API for lab work",
    version="1.0.0",
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# LAB 04:
# Middleware пока заглушка. Студенты реализуют логику идемпотентности внутри dispatch().
app.add_middleware(IdempotencyMiddleware)

# Include routes
app.include_router(router, prefix="/api")
app.include_router(payment_router)  # Payment routes для тестирования конкурентности


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}
