import hashlib
import json
from typing import Any
from fastapi import UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.exceptions import (
    LockAcquisitionException,
    MessageValidationException,
    StorageUnavailableException,
)
from app.core.redis import (
    acquire_lock,
    check_rate_limit,
    check_spam,
    invalidate_message_cache,
    peek_rate_limit,
    publish_message,
    redis_client,
    release_lock,
)
from app.models.models import Message, User
from app.schemas.schemas import MessageOut
from app.services.storage_service import StorageService


class MessageService:
    """Rich service orchestrating message persistence, validation, caching, anti-spam, storage, and real-time events."""

    def __init__(self, db: AsyncSession, storage: StorageService) -> None:
        self.db = db
        self.storage = storage

    def format_message(self, message: Message) -> MessageOut:
        """Convert a database Message entity to a client-ready MessageOut schema with resolved media URLs."""
        return MessageOut(
            id=message.id,
            username=message.username,
            text=message.text or "",
            file_url=self.storage.get_url(message.file_key),
            thumbnail_url=self.storage.get_url(message.thumbnail_key),
            file_name=message.file_name,
            file_content_type=message.file_content_type,
            file_size=message.file_size,
            created_at=message.created_at,
        )

    def serialize_messages(self, messages: list[Message]) -> list[dict[str, Any]]:
        """Serialize a list of Message entities into JSON-compatible dictionaries."""
        return [jsonable_encoder(self.format_message(msg)) for msg in messages]

    async def get_messages_page(self, limit: int = 50, before_id: int | None = None) -> list[dict[str, Any]]:
        """Retrieve paginated messages using Redis cache with fallback to database."""
        cache_key = f"messages:page:{limit}:{before_id or 0}"

        cached = await redis_client.get(cache_key)
        if cached:
            try:
                return json.loads(cached)
            except Exception:
                pass

        query = select(Message)
        if before_id is not None:
            query = query.where(Message.id < before_id)

        query = query.order_by(Message.id.desc()).limit(limit)
        result = await self.db.execute(query)
        messages = list(result.scalars().all())
        messages.reverse()

        payload = self.serialize_messages(messages)

        await redis_client.set(
            cache_key,
            json.dumps(payload, ensure_ascii=False),
            ex=settings.MESSAGE_CACHE_TTL_SECONDS,
        )

        return payload

    async def precheck_rate_limit(self, user_id: int) -> None:
        """Pre-check rate limit before heavy operations like file uploading."""
        await peek_rate_limit(user_id)

    async def read_upload(self, file: UploadFile, max_size: int = settings.MAX_FILE_SIZE) -> bytes:
        """Stream and read uploaded file in chunks, enforcing maximum size constraints."""
        chunks = []
        total = 0

        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break

            total += len(chunk)
            if total > max_size:
                raise MessageValidationException("Файл слишком большой", status_code=413)
            chunks.append(chunk)

        return b"".join(chunks)

    async def create_message(
        self,
        user: User,
        text: str,
        file_data: bytes | None = None,
        file_name: str | None = None,
        file_content_type: str | None = None,
    ) -> MessageOut:
        """Validate, rate-limit, save, upload media, publish, and dispatch background tasks for a new message."""
        cleaned_text = (text or "").strip()

        if len(cleaned_text) > settings.MAX_TEXT_LENGTH:
            raise MessageValidationException("Текст слишком длинный", status_code=413)

        if not cleaned_text and file_data is None:
            raise MessageValidationException("Сообщение должно содержать текст или файл", status_code=400)

        if file_data is not None:
            if len(file_data) == 0:
                raise MessageValidationException("Файл пустой", status_code=400)

            if len(file_data) > settings.MAX_FILE_SIZE:
                raise MessageValidationException("Файл слишком большой", status_code=413)

            if not self.storage.is_available:
                raise StorageUnavailableException()

        # Enforce rate limiting
        await check_rate_limit(user.id)

        # Enforce anti-spam / duplicate message check
        file_sha256 = hashlib.sha256(file_data).hexdigest() if file_data else None
        dummy_msg = Message(text=cleaned_text)
        fingerprint = dummy_msg.compute_fingerprint(file_sha256)
        await check_spam(user.id, fingerprint)

        # Acquire distributed user lock
        lock_name = f"lock:send:{user.id}"
        lock_token = await acquire_lock(lock_name, timeout_ms=15000)

        if not lock_token:
            raise LockAcquisitionException("Слишком много одновременных запросов")

        try:
            file_key = None
            file_size = None

            if file_data is not None:
                file_key = self.storage.generate_file_key(file_name)
                file_size = len(file_data)
                await self.storage.upload(
                    key=file_key,
                    data=file_data,
                    content_type=file_content_type or "application/octet-stream",
                )

            message = Message(
                user_id=user.id,
                username=user.username,
                text=cleaned_text,
                file_key=file_key,
                file_name=file_name,
                file_content_type=file_content_type,
                file_size=file_size,
            )
            self.db.add(message)
            await self.db.commit()
            await self.db.refresh(message)

            out = self.format_message(message)
            payload = jsonable_encoder(out)

            # Invalidate page caches and broadcast message to Redis Pub/Sub
            await invalidate_message_cache()
            await publish_message(payload)

            # Dispatch background celery tasks
            try:
                from app.tasks.tasks import process_message

                await run_in_threadpool(process_message.delay, message.id)
            except Exception:
                pass

            return out
        finally:
            await release_lock(lock_name, lock_token)

    async def get_notifications(self, limit: int = 50) -> list[dict[str, Any]]:
        """Retrieve recent broadcast notifications from Redis."""
        items = await redis_client.lrange("chat:notifications", 0, limit - 1)
        result = []
        for item in items:
            try:
                result.append(json.loads(item))
            except Exception:
                continue
        return result
