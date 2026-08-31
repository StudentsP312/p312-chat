from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from app.core.config import settings

# --- Async Engine & Session (FastAPI / WebSockets / Native Async) ---
async_engine_kwargs: dict = {
    "pool_pre_ping": True,
}

if settings.ASYNC_DATABASE_URL.startswith("sqlite"):
    async_engine_kwargs["connect_args"] = {"check_same_thread": False}
    if ":///" in settings.ASYNC_DATABASE_URL:
        db_path = settings.ASYNC_DATABASE_URL.split(":///", 1)[1]
        if db_path and db_path != ":memory:":
            Path(db_path).resolve().parent.mkdir(parents=True, exist_ok=True)
else:
    async_engine_kwargs.update(
        {
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "pool_timeout": settings.DB_POOL_TIMEOUT,
            "pool_recycle": settings.DB_POOL_RECYCLE,
        }
    )

async_engine = create_async_engine(
    settings.ASYNC_DATABASE_URL,
    **async_engine_kwargs,
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    autoflush=False,
    expire_on_commit=False,
)


# --- Sync Engine & Session (Celery Workers / CLI Tools) ---
sync_engine_kwargs: dict = {
    "pool_pre_ping": True,
}

if settings.SYNC_DATABASE_URL.startswith("sqlite"):
    sync_engine_kwargs["connect_args"] = {"check_same_thread": False}
else:
    sync_engine_kwargs.update(
        {
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "pool_timeout": settings.DB_POOL_TIMEOUT,
            "pool_recycle": settings.DB_POOL_RECYCLE,
        }
    )

sync_engine = create_engine(
    settings.SYNC_DATABASE_URL,
    **sync_engine_kwargs,
)

SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    autoflush=False,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for providing an async database session per request."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def get_sync_db() -> Generator[Session, None, None]:
    """Helper for providing a sync database session (Celery / scripts)."""
    db = SyncSessionLocal()
    try:
        yield db
    finally:
        db.close()


# Compatibility aliases
engine = async_engine
SessionLocal = AsyncSessionLocal
