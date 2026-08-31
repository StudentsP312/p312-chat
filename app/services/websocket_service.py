import asyncio
import json
from typing import Any
from fastapi import WebSocket, WebSocketDisconnect
from app.core.exceptions import AppException
from app.core.redis import redis_client
from app.services.auth_service import AuthService
from app.services.message_service import MessageService


class ConnectionManager:
    """Manages active local WebSocket connections and broadcasting."""

    def __init__(self) -> None:
        self.active_websockets: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_websockets.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_websockets:
            self.active_websockets.remove(websocket)

    async def broadcast_local(self, payload: dict[str, Any]) -> None:
        dead: list[WebSocket] = []
        for websocket in list(self.active_websockets):
            try:
                await websocket.send_json(payload)
            except Exception:
                dead.append(websocket)

        for websocket in dead:
            self.disconnect(websocket)


class WebSocketService:
    """Service handling WebSocket real-time chat lifecycle, pub/sub bridge, and client messaging."""

    def __init__(self) -> None:
        self.manager = ConnectionManager()

    async def redis_listener(self) -> None:
        """Subscribe to Redis Pub/Sub channel and broadcast incoming messages to local active connections."""
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

                await self.manager.broadcast_local(payload)
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
                pass

    async def handle_connection(
        self,
        websocket: WebSocket,
        token: str | None,
        auth_service: AuthService,
        message_service: MessageService,
    ) -> None:
        """Handle full lifecycle of a client WebSocket session."""
        if not token:
            await websocket.close(code=1008)
            return

        try:
            user, _ = await auth_service.validate_session(token)
        except Exception:
            await websocket.close(code=1008)
            return

        await self.manager.connect(websocket)

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
                    await message_service.create_message(
                        user=user,
                        text=text,
                    )
                except AppException as exc:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "detail": exc.message,
                        }
                    )
                except Exception as exc:
                    await websocket.send_json(
                        {
                            "type": "error",
                            "detail": str(exc),
                        }
                    )
        except WebSocketDisconnect:
            self.manager.disconnect(websocket)
        except Exception:
            self.manager.disconnect(websocket)


ws_service = WebSocketService()
