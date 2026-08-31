from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from app.api.deps import get_health_service
from app.services.health_service import HealthService

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(health_service: HealthService = Depends(get_health_service)):
    result, code = await health_service.check_health()
    return JSONResponse(status_code=code, content=result)
