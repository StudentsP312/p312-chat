from typing import Any
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.redis import redis_client
from app.services.storage_service import StorageService


class HealthService:
    """Service checking the operational status of all critical infrastructure components."""

    def __init__(self, db: AsyncSession, storage: StorageService) -> None:
        self.db = db
        self.storage = storage

    async def check_health(self) -> tuple[dict[str, Any], int]:
        """Perform health checks on database, Redis, and S3 storage."""
        result: dict[str, str] = {
            "api": "ok",
            "database": "ok",
            "redis": "ok",
            "s3": "ok",
        }
        status_code = 200

        # Database Check
        try:
            await self.db.execute(text("SELECT 1"))
        except Exception:
            result["database"] = "error"
            status_code = 503

        # Redis Check
        try:
            await redis_client.ping()
        except Exception:
            result["redis"] = "error"
            status_code = 503

        # S3 Check
        if not settings.S3_BUCKET:
            result["s3"] = "not_configured"
        elif not settings.HEALTH_CHECK_S3:
            result["s3"] = "skipped"
        else:
            try:
                await run_in_threadpool(self.storage.check_health_sync)
            except Exception:
                result["s3"] = "error"
                status_code = 503

        return result, status_code
