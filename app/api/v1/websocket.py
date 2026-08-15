import asyncio
import json
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from app.api.deps import authenticate_token
from app.api.v1.messages import create_message_core
from app.core.redis import redis_client

router = APIRouter(tags=["websocket"])


class ConnectionManager:
    def __init__(self):
        self.active_websockets: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_websockets.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_websockets:
            self.active_websockets.remove(websocket)

    async def broadcast_local(self, payload: dict) -> None:
        dead = []
        for websocket in self.active_websockets:
            try:
                await websocket.send_json(payload)
            except Exception:
                dead.append(websocket)

        for websocket in dead:
            self.disconnect(websocket)


manager = ConnectionManager()


async def redis_listener() -> None:
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("chat:messages")

    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue

            data = message.get("data")
            if not data:
                continue

            try:
                payload = json.loads(data)
            except Exception:
                continue

            await manager.broadcast_local(payload)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            await pubsub.unsubscribe("chat:messages")
        except Exception:
            pass

        try:
            await pubsub.close()
        except Exception:
            try:
                pubsub.close()
            except Exception:
                pass


@router.websocket("/ws")
async def websocket_chat(websocket: WebSocket):
    token = websocket.query_params.get("token")

    if not token:
        await websocket.close(code=1008)
        return

    user = await authenticate_token(token)

    if not user:
        await websocket.close(code=1008)
        return

    await manager.connect(websocket)

    try:
        await websocket.send_json(
            {
                "type": "connected",
                "username": user.username,
            }
        )

        while True:
            raw = await websocket.receive_text()

            try:
                data = json.loads(raw)
            except Exception:
                await websocket.send_json(
                    {
                        "type": "error",
                        "detail": "Некорректный JSON",
                    }
                )
                continue

            text = str(data.get("text", ""))

            try:
                await create_message_core(
                    user=user,
                    text=text,
                    file_data=None,
                    file_name=None,
                    file_content_type=None,
                )
            except HTTPException as exc:
                await websocket.send_json(
                    {
                        "type": "error",
                        "detail": exc.detail,
                    }
                )
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
