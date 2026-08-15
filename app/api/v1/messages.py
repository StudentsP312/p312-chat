import hashlib
import json
import os
import re
import uuid
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import AuthContext, get_current_auth
from app.core.config import settings
from app.core.database import AsyncSessionLocal, get_db
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
from app.core.s3 import build_object_url, s3_client, upload_to_s3
from app.models.models import Message, User
from app.schemas.schemas import MessageOut
from app.tasks.tasks import process_message

router = APIRouter(tags=["messages"])


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


async def get_messages_page(db: AsyncSession, limit: int, before_id: int | None) -> list[Message]:
    query = select(Message)
    if before_id is not None:
        query = query.where(Message.id < before_id)

    query = query.order_by(Message.id.desc()).limit(limit)
    result = await db.execute(query)
    messages = list(result.scalars().all())
    messages.reverse()
    return messages


async def save_message(
    db: AsyncSession,
    user_id: int,
    username: str,
    text: str,
    file_key: str | None,
    file_name: str | None,
    file_content_type: str | None,
    file_size: int | None,
) -> Message:
    message = Message(
        user_id=user_id,
        username=username,
        text=text,
        file_key=file_key,
        file_name=file_name,
        file_content_type=file_content_type,
        file_size=file_size,
    )
    db.add(message)
    await db.commit()
    await db.refresh(message)
    return message


def message_to_out(message: Message) -> MessageOut:
    return MessageOut(
        id=message.id,
        username=message.username,
        text=message.text or "",
        file_url=build_object_url(message.file_key),
        thumbnail_url=build_object_url(message.thumbnail_key),
        file_name=message.file_name,
        file_content_type=message.file_content_type,
        file_size=message.file_size,
        created_at=message.created_at,
    )


def serialize_messages(messages: list[Message]) -> list[dict]:
    return [jsonable_encoder(message_to_out(message)) for message in messages]


async def read_upload_with_limit(file: UploadFile, limit: int) -> bytes:
    chunks = []
    total = 0

    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break

        total += len(chunk)
        if total > limit:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Файл слишком большой",
            )
        chunks.append(chunk)

    return b"".join(chunks)


async def create_message_core(
    user: User,
    text: str,
    file_data: bytes | None = None,
    file_name: str | None = None,
    file_content_type: str | None = None,
    db: AsyncSession | None = None,
) -> MessageOut:
    cleaned_text = (text or "").strip()

    if len(cleaned_text) > settings.MAX_TEXT_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Текст слишком длинный",
        )

    if not cleaned_text and file_data is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Сообщение должно содержать текст или файл",
        )

    if file_data is not None:
        if len(file_data) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Файл пустой",
            )

        if len(file_data) > settings.MAX_FILE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Файл слишком большой",
            )

        if not s3_client or not settings.S3_BUCKET:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Хранилище файлов не настроено",
            )

    await check_rate_limit(user.id)

    normalized = normalize_text(cleaned_text)
    if file_data is not None:
        file_sha256 = hashlib.sha256(file_data).hexdigest()
        fingerprint = f"text={normalized};file={file_sha256}"
    else:
        fingerprint = f"text={normalized}"

    await check_spam(user.id, fingerprint)

    lock_name = f"lock:send:{user.id}"
    lock_token = await acquire_lock(lock_name, 15000)

    if not lock_token:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много одновременных запросов",
        )

    try:
        file_key = None
        file_size = None

        if file_data is not None:
            raw_ext = os.path.splitext(file_name or "")[1].lower()
            file_ext = re.sub(r"[^a-z0-9.]", "", raw_ext)[:20]
            file_key = f"chat/{uuid.uuid4().hex}{file_ext}"
            file_size = len(file_data)

            await run_in_threadpool(
                upload_to_s3,
                file_key,
                file_data,
                file_content_type or "application/octet-stream",
            )

        if db is not None:
            message = await save_message(
                db,
                user.id,
                user.username,
                cleaned_text,
                file_key,
                file_name,
                file_content_type,
                file_size,
            )
        else:
            async with AsyncSessionLocal() as session:
                message = await save_message(
                    session,
                    user.id,
                    user.username,
                    cleaned_text,
                    file_key,
                    file_name,
                    file_content_type,
                    file_size,
                )

        out = message_to_out(message)
        payload = jsonable_encoder(out)

        await invalidate_message_cache()
        await publish_message(payload)

        try:
            await run_in_threadpool(process_message.delay, message.id)
        except Exception:
            pass

        return out
    finally:
        await release_lock(lock_name, lock_token)


@router.get("/messages", response_model=list[MessageOut])
async def get_messages(
    limit: int = Query(default=50, ge=1, le=100),
    before_id: int | None = Query(default=None),
    auth: AuthContext = Depends(get_current_auth),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"messages:page:{limit}:{before_id or 0}"

    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass

    messages = await get_messages_page(db, limit, before_id)
    payload = serialize_messages(messages)

    await redis_client.set(
        cache_key,
        json.dumps(payload, ensure_ascii=False),
        ex=settings.MESSAGE_CACHE_TTL_SECONDS,
    )

    return payload


@router.post("/messages", status_code=status.HTTP_201_CREATED, response_model=MessageOut)
async def create_message_endpoint(
    text: str = Form(default=""),
    file: UploadFile | None = File(default=None),
    auth: AuthContext = Depends(get_current_auth),
    db: AsyncSession = Depends(get_db),
):
    user = auth.user
    has_upload = file is not None and bool(file.filename)

    if has_upload and (not s3_client or not settings.S3_BUCKET):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Хранилище файлов не настроено",
        )

    await peek_rate_limit(user.id)

    file_data = None
    file_name = None
    file_content_type = None

    if has_upload and file is not None:
        file_data = await read_upload_with_limit(file, settings.MAX_FILE_SIZE)
        if len(file_data) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Файл пустой",
            )
        file_name = file.filename
        file_content_type = file.content_type or "application/octet-stream"

    return await create_message_core(
        user=user,
        text=text,
        file_data=file_data,
        file_name=file_name,
        file_content_type=file_content_type,
        db=db,
    )


@router.get("/notifications")
async def get_notifications(auth: AuthContext = Depends(get_current_auth)):
    items = await redis_client.lrange("chat:notifications", 0, 49)
    result = []
    for item in items:
        try:
            result.append(json.loads(item))
        except Exception:
            continue
    return result
