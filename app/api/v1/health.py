from fastapi import APIRouter, Depends, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.database import get_db
from app.core.redis import redis_client
from app.core.s3 import check_s3_sync

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(db: AsyncSession = Depends(get_db)):
    result = {
        "api": "ok",
        "database": "ok",
        "redis": "ok",
        "s3": "ok",
    }

    code = status.HTTP_200_OK

    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        result["database"] = "error"
        code = status.HTTP_503_SERVICE_UNAVAILABLE

    try:
        await redis_client.ping()
    except Exception:
        result["redis"] = "error"
        code = status.HTTP_503_SERVICE_UNAVAILABLE

    if not settings.S3_BUCKET:
        result["s3"] = "not_configured"
    elif not settings.HEALTH_CHECK_S3:
        result["s3"] = "skipped"
    else:
        try:
            await run_in_threadpool(check_s3_sync)
        except Exception:
            result["s3"] = "error"
            code = status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(status_code=code, content=result)
