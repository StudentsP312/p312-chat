from fastapi import APIRouter, WebSocket
from app.core.database import AsyncSessionLocal
from app.services.auth_service import AuthService
from app.services.message_service import MessageService
from app.services.storage_service import StorageService
from app.services.websocket_service import ws_service

router = APIRouter(tags=["websocket"])

redis_listener = ws_service.redis_listener


@router.websocket("/ws")
async def websocket_chat(websocket: WebSocket):
    token = websocket.query_params.get("token")

    async with AsyncSessionLocal() as session:
        storage_service = StorageService()
        auth_service = AuthService(session)
        message_service = MessageService(session, storage_service)

        await ws_service.handle_connection(
            websocket=websocket,
            token=token,
            auth_service=auth_service,
            message_service=message_service,
        )
