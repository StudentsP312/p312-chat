# app.py
#
# pip install fastapi uvicorn sqlalchemy python-multipart boto3 pyjwt redis celery pillow celery-redbeat
#
# API:
# uvicorn app:app --host 0.0.0.0 --port 8000
#
# Worker:
# celery -A app.celery_app worker -l info
#
# Beat можно запускать на обеих машинах:
# celery -A app.celery_app beat -l info

import asyncio
import hashlib
import io
import json
import os
import re
import secrets
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import boto3
import jwt
import redis.asyncio as aioredis
from celery import Celery
from celery.schedules import crontab
from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from redis import Redis as SyncRedis
from redbeat import RedBeatSchedulerEntry
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, create_engine, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker


SECRET_KEY = os.getenv("JWT_SECRET", secrets.token_hex(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))
ACCESS_TOKEN_EXPIRE_SECONDS = ACCESS_TOKEN_EXPIRE_MINUTES * 60

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./chat.db")

RATE_LIMIT_MS = max(1, int(float(os.getenv("RATE_LIMIT_SECONDS", "5")) * 1000))
MAX_CONSECUTIVE_SAME_MESSAGES = max(1, int(os.getenv("MAX_CONSECUTIVE_SAME_MESSAGES", "2")))
SPAM_TTL_SECONDS = max(1, int(os.getenv("SPAM_TTL_SECONDS", "3600")))
MAX_TEXT_LENGTH = int(os.getenv("MAX_TEXT_LENGTH", "4000"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(20 * 1024 * 1024)))
MESSAGE_CACHE_TTL_SECONDS = max(1, int(os.getenv("MESSAGE_CACHE_TTL_SECONDS", "2")))

HEALTH_CHECK_S3 = os.getenv("HEALTH_CHECK_S3", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
S3_REGION_NAME = os.getenv("S3_REGION_NAME") or None
S3_PUBLIC_BASE_URL = os.getenv("S3_PUBLIC_BASE_URL") or None

s3_client = None
if S3_BUCKET:
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION_NAME,
    )

engine_kwargs = {}
if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(32), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    username = Column(String(32), nullable=False)
    text = Column(Text, default="", nullable=False)
    file_key = Column(String(512), nullable=True)
    thumbnail_key = Column(String(512), nullable=True)
    file_name = Column(String(255), nullable=True)
    file_content_type = Column(String(255), nullable=True)
    file_size = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)


Base.metadata.create_all(bind=engine)

redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
redis_sync = SyncRedis.from_url(REDIS_URL, decode_responses=True)

celery_app = Celery("chat_worker", broker=REDIS_URL, backend=REDIS_URL)
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    beat_scheduler="redbeat.RedBeatScheduler",
    redbeat_redis_url=REDIS_URL,
    redbeat_lock_timeout=25,
    redbeat_key_prefix="redbeat:",
)


@celery_app.task(name="notify_message")
def notify_message(message_id: int):
    db = SessionLocal()
    try:
        message = db.get(Message, message_id)
        if not message:
            return "not_found"

        payload = {
            "id": message.id,
            "username": message.username,
            "text": message.text or "",
            "has_file": bool(message.file_key),
            "created_at": message.created_at.isoformat() if message.created_at else None,
        }

        redis_sync.lpush("chat:notifications", json.dumps(payload, ensure_ascii=False))
        redis_sync.ltrim("chat:notifications", 0, 999)
        return "ok"
    finally:
        db.close()


@celery_app.task(name="process_file_thumbnail")
def process_file_thumbnail(message_id: int):
    db = SessionLocal()
    try:
        message = db.get(Message, message_id)
        if not message or not message.file_key:
            return "no_file"

        if not message.file_content_type or not message.file_content_type.startswith("image/"):
            return "not_image"

        if not s3_client or not S3_BUCKET:
            return "s3_not_configured"

        try:
            from PIL import Image
        except Exception:
            return "pillow_not_installed"

        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=message.file_key)
            data = obj["Body"].read()

            image = Image.open(io.BytesIO(data))
            image.thumbnail((128, 128))

            fmt = (image.format or "PNG").upper()
            if fmt in ("JPEG", "JPG") and image.mode not in ("RGB", "L"):
                image = image.convert("RGB")

            buffer = io.BytesIO()
            image.save(buffer, format=fmt)

            thumbnail_key = f"thumbnails/{message.file_key}"
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=thumbnail_key,
                Body=buffer.getvalue(),
                ContentType=message.file_content_type,
            )

            message.thumbnail_key = thumbnail_key
            db.commit()
            return "ok"
        except Exception as exc:
            return f"error: {exc}"
    finally:
        db.close()


@celery_app.task(name="cleanup_old_messages")
def cleanup_old_messages():
    retention_days = int(os.getenv("MESSAGE_RETENTION_DAYS", "30"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    db = SessionLocal()
    try:
        old_messages = db.query(Message).filter(Message.created_at < cutoff).all()
        if not old_messages:
            return 0

        keys_to_delete = []

        for message in old_messages:
            if message.file_key:
                keys_to_delete.append(message.file_key)
            if message.thumbnail_key:
                keys_to_delete.append(message.thumbnail_key)
            db.delete(message)

        if keys_to_delete and s3_client and S3_BUCKET:
            try:
                for i in range(0, len(keys_to_delete), 1000):
                    chunk = keys_to_delete[i:i + 1000]
                    s3_client.delete_objects(
                        Bucket=S3_BUCKET,
                        Delete={"Objects": [{"Key": key} for key in chunk]},
                    )
            except Exception:
                pass

        db.commit()

        try:
            for key in redis_sync.scan_iter(match="messages:page:*"):
                redis_sync.delete(key)
        except Exception:
            pass

        return len(old_messages)
    finally:
        db.close()


@celery_app.task(name="process_message")
def process_message(message_id: int):
    notify_message.delay(message_id)
    process_file_thumbnail.delay(message_id)
    return "queued"


def ensure_beat_schedule():
    try:
        if redis_sync.exists("redbeat:bootstrap:v1"):
            return

        RedBeatSchedulerEntry(
            name="cleanup-old-messages-hourly",
            task="cleanup_old_messages",
            schedule=crontab(minute=0),
            app=celery_app,
        ).save()

        redis_sync.set("redbeat:bootstrap:v1", "1")
    except Exception:
        pass


ensure_beat_schedule()


SPAM_LUA = """
local key = KEYS[1]
local fingerprint = ARGV[1]
local max_count = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'f', 'c')
local current_fingerprint = data[1]
local current_count = tonumber(data[2]) or 0

local new_count
if current_fingerprint == fingerprint then
    new_count = current_count + 1
else
    new_count = 1
end

if new_count > max_count then
    redis.call('HSET', key, 'f', fingerprint, 'c', new_count)
    redis.call('EXPIRE', key, ttl)
    return 0
end

redis.call('HSET', key, 'f', fingerprint, 'c', new_count)
redis.call('EXPIRE', key, ttl)
return 1
"""

RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""

spam_script = redis_client.register_script(SPAM_LUA)
release_lock_script = redis_client.register_script(RELEASE_LOCK_LUA)


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=8, max_length=128)


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MessageOut(BaseModel):
    id: int
    username: str
    text: str
    file_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    file_name: Optional[str] = None
    file_content_type: Optional[str] = None
    file_size: Optional[int] = None
    created_at: datetime


class PasswordResetRequest(BaseModel):
    username: str = Field(min_length=3, max_length=32)


class PasswordResetConfirm(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    code: str = Field(min_length=6, max_length=16)
    new_password: str = Field(min_length=8, max_length=128)


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return f"{salt.hex()}${derived.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, hash_hex = stored.split("$", 1)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except Exception:
        return False

    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return secrets.compare_digest(derived, expected)


def create_access_token(user_id: int, username: str):
    jti = uuid.uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ACCESS_TOKEN_EXPIRE_SECONDS)

    payload = {
        "sub": str(user_id),
        "username": username,
        "jti": jti,
        "exp": expires_at,
    }

    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token, jti, ACCESS_TOKEN_EXPIRE_SECONDS


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


def get_user_by_id_sync(user_id: int) -> Optional[User]:
    db = SessionLocal()
    try:
        return db.query(User).filter(User.id == user_id).first()
    finally:
        db.close()


def get_user_by_username_sync(username: str) -> Optional[User]:
    db = SessionLocal()
    try:
        return db.query(User).filter(User.username == username).first()
    finally:
        db.close()


def create_user_sync(username: str, password: str) -> User:
    db = SessionLocal()
    try:
        exists = db.query(User).filter(User.username == username).first()
        if exists:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Имя пользователя уже занято",
            )

        user = User(
            username=username,
            hashed_password=hash_password(password),
        )

        db.add(user)
        db.commit()
        db.refresh(user)
        return user
    finally:
        db.close()


def authenticate_user_sync(username: str, password: str) -> Optional[User]:
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == username).first()
        if not user or not verify_password(password, user.hashed_password):
            return None
        return user
    finally:
        db.close()


def update_password_sync(username: str, new_password: str) -> Optional[int]:
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == username).first()
        if not user:
            return None

        user.hashed_password = hash_password(new_password)
        db.commit()
        return user.id
    finally:
        db.close()


def get_messages_page_sync(limit: int, before_id: Optional[int]) -> List[Message]:
    db = SessionLocal()
    try:
        query = db.query(Message)

        if before_id is not None:
            query = query.filter(Message.id < before_id)

        messages = query.order_by(Message.id.desc()).limit(limit).all()
        messages.reverse()
        return messages
    finally:
        db.close()


def save_message(
    user_id: int,
    username: str,
    text: str,
    file_key: Optional[str],
    file_name: Optional[str],
    file_content_type: Optional[str],
    file_size: Optional[int],
) -> Message:
    db = SessionLocal()
    try:
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
        db.commit()
        db.refresh(message)
        return message
    finally:
        db.close()


def check_db_sync():
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
    finally:
        db.close()


def check_s3_sync():
    if s3_client and S3_BUCKET:
        s3_client.head_bucket(Bucket=S3_BUCKET)


def build_object_url(key: Optional[str]) -> Optional[str]:
    if not key:
        return None

    if S3_PUBLIC_BASE_URL:
        return f"{S3_PUBLIC_BASE_URL.rstrip('/')}/{key}"

    if not s3_client or not S3_BUCKET:
        return None

    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": key,
            },
            ExpiresIn=3600,
        )
    except Exception:
        return None


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


def serialize_messages_sync(messages: List[Message]) -> List[dict]:
    return [jsonable_encoder(message_to_out(message)) for message in messages]


def upload_to_s3(key: str, data: bytes, content_type: str) -> None:
    if not s3_client or not S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Хранилище файлов не настроено",
        )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


@dataclass
class AuthContext:
    user: User
    jti: str


async def get_current_auth(token: str = Depends(oauth2_scheme)) -> AuthContext:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Не удалось проверить токен",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
        jti = payload.get("jti")
    except Exception:
        raise credentials_exception

    if not jti:
        raise credentials_exception

    session_exists = await redis_client.exists(f"session:{jti}")
    if not session_exists:
        raise credentials_exception

    user = await run_in_threadpool(get_user_by_id_sync, user_id)
    if user is None:
        raise credentials_exception

    return AuthContext(user=user, jti=jti)


def get_current_user(auth: AuthContext = Depends(get_current_auth)) -> User:
    return auth.user


async def authenticate_token(token: str) -> Optional[User]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
        jti = payload.get("jti")
    except Exception:
        return None

    if not jti:
        return None

    session_exists = await redis_client.exists(f"session:{jti}")
    if not session_exists:
        return None

    return await run_in_threadpool(get_user_by_id_sync, user_id)


async def peek_rate_limit(user_id: int) -> None:
    key = f"ratelimit:messages:{user_id}"
    exists = await redis_client.exists(key)

    if exists:
        pttl = await redis_client.pttl(key)
        retry_after = max(1, int((pttl + 999) / 1000)) if pttl and pttl > 0 else 1

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком быстро. Одно сообщение в 5 секунд.",
            headers={"Retry-After": str(retry_after)},
        )


async def check_rate_limit(user_id: int) -> None:
    key = f"ratelimit:messages:{user_id}"

    ok = await redis_client.set(key, "1", nx=True, px=RATE_LIMIT_MS)
    if ok:
        return

    pttl = await redis_client.pttl(key)
    retry_after = max(1, int((pttl + 999) / 1000)) if pttl and pttl > 0 else 1

    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Слишком быстро. Одно сообщение в 5 секунд.",
        headers={"Retry-After": str(retry_after)},
    )


async def check_spam(user_id: int, fingerprint: str) -> None:
    key = f"spam:{user_id}"

    allowed = await spam_script(
        keys=[key],
        args=[fingerprint, MAX_CONSECUTIVE_SAME_MESSAGES, SPAM_TTL_SECONDS],
    )

    if allowed != 1:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Нельзя отправлять одно и то же сообщение больше двух раз подряд.",
        )


async def acquire_lock(name: str, timeout_ms: int = 15000) -> Optional[str]:
    token = uuid.uuid4().hex
    ok = await redis_client.set(name, token, nx=True, px=timeout_ms)

    if ok:
        return token

    return None


async def release_lock(name: str, token: str) -> None:
    await release_lock_script(keys=[name], args=[token])


async def invalidate_message_cache() -> None:
    cursor = 0
    while True:
        cursor, keys = await redis_client.scan(cursor=cursor, match="messages:page:*", count=100)
        if keys:
            await redis_client.delete(*keys)
        if cursor == 0:
            break


async def publish_message(payload: dict) -> None:
    await redis_client.publish("chat:messages", json.dumps(payload, ensure_ascii=False))


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
    file_data: Optional[bytes] = None,
    file_name: Optional[str] = None,
    file_content_type: Optional[str] = None,
) -> MessageOut:
    cleaned_text = (text or "").strip()

    if len(cleaned_text) > MAX_TEXT_LENGTH:
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

        if len(file_data) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Файл слишком большой",
            )

        if not s3_client or not S3_BUCKET:
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

        message = await run_in_threadpool(
            save_message,
            user.id,
            user.username,
            cleaned_text,
            file_key,
            file_name,
            file_content_type,
            file_size,
        )

        out = await run_in_threadpool(message_to_out, message)
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


class ConnectionManager:
    def __init__(self):
        self.active_websockets: List[WebSocket] = []

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(ensure_beat_schedule)
    listener_task = asyncio.create_task(redis_listener())
    yield
    listener_task.cancel()

    try:
        await listener_task
    except asyncio.CancelledError:
        pass

    try:
        await redis_client.aclose()
    except Exception:
        pass


app = FastAPI(title="Public chat API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    result = {
        "api": "ok",
        "database": "ok",
        "redis": "ok",
        "s3": "ok",
    }

    code = status.HTTP_200_OK

    try:
        await run_in_threadpool(check_db_sync)
    except Exception:
        result["database"] = "error"
        code = status.HTTP_503_SERVICE_UNAVAILABLE

    try:
        await redis_client.ping()
    except Exception:
        result["redis"] = "error"
        code = status.HTTP_503_SERVICE_UNAVAILABLE

    if not S3_BUCKET:
        result["s3"] = "not_configured"
    elif not HEALTH_CHECK_S3:
        result["s3"] = "skipped"
    else:
        try:
            await run_in_threadpool(check_s3_sync)
        except Exception:
            result["s3"] = "error"
            code = status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(status_code=code, content=result)


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(payload: UserCreate):
    username = payload.username.strip().lower()

    if not re.fullmatch(r"[a-z0-9_]{3,32}", username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Имя пользователя может содержать строчные латинские буквы, цифры и символ подчеркивания.",
        )

    user = await run_in_threadpool(create_user_sync, username, payload.password)

    return {
        "id": user.id,
        "username": user.username,
    }


@app.post("/auth/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    username = form_data.username.strip().lower()

    user = await run_in_threadpool(authenticate_user_sync, username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверное имя пользователя или пароль",
        )

    token, jti, ttl = create_access_token(user.id, user.username)

    await redis_client.set(f"session:{jti}", str(user.id), ex=ttl)
    await redis_client.sadd(f"user_sessions:{user.id}", jti)
    await redis_client.expire(f"user_sessions:{user.id}", ttl)

    return Token(access_token=token)


@app.post("/auth/logout")
async def logout(auth: AuthContext = Depends(get_current_auth)):
    await redis_client.delete(f"session:{auth.jti}")
    await redis_client.srem(f"user_sessions:{auth.user.id}", auth.jti)

    return {"ok": True}


@app.post("/auth/logout-all")
async def logout_all(auth: AuthContext = Depends(get_current_auth)):
    sessions = await redis_client.smembers(f"user_sessions:{auth.user.id}")

    if sessions:
        keys = [f"session:{jti}" for jti in sessions]
        await redis_client.delete(*keys)

    await redis_client.delete(f"user_sessions:{auth.user.id}")

    return {"ok": True}


@app.post("/auth/password-reset/request")
async def request_password_reset(payload: PasswordResetRequest):
    username = payload.username.strip().lower()

    rate_key = f"ratelimit:password_reset:{username}"
    ok = await redis_client.set(rate_key, "1", nx=True, px=60_000)

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много запросов сброса пароля",
        )

    user = await run_in_threadpool(get_user_by_username_sync, username)

    if user:
        code = f"{secrets.randbelow(1000000):06d}"
        await redis_client.set(f"reset:{username}", hash_password(code), ex=300)

        return {
            "detail": "Код сброса создан",
            "debug_code": code,
        }

    return {
        "detail": "Если пользователь существует, код сброса отправлено",
    }


@app.post("/auth/password-reset/confirm")
async def confirm_password_reset(payload: PasswordResetConfirm):
    username = payload.username.strip().lower()

    lock_name = f"lock:password_reset:{username}"
    lock_token = await acquire_lock(lock_name, 10000)

    if not lock_token:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много одновременных запросов сброса",
        )

    try:
        attempts_key = f"reset_attempts:{username}"
        attempts = await redis_client.incr(attempts_key)
        await redis_client.expire(attempts_key, 300)

        if attempts > 5:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Слишком много попыток подтверждения кода",
            )

        stored = await redis_client.get(f"reset:{username}")
        if not stored or not verify_password(payload.code.strip(), stored):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Неверный код сброса",
            )

        user_id = await run_in_threadpool(update_password_sync, username, payload.new_password)
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Пользователь не найден",
            )

        await redis_client.delete(f"reset:{username}")
        await redis_client.delete(attempts_key)

        sessions = await redis_client.smembers(f"user_sessions:{user_id}")
        if sessions:
            keys = [f"session:{jti}" for jti in sessions]
            await redis_client.delete(*keys)

        await redis_client.delete(f"user_sessions:{user_id}")

        return {"ok": True}
    finally:
        await release_lock(lock_name, lock_token)


@app.get("/messages", response_model=List[MessageOut])
async def get_messages(
    limit: int = Query(default=50, ge=1, le=100),
    before_id: Optional[int] = Query(default=None),
    auth: AuthContext = Depends(get_current_auth),
):
    cache_key = f"messages:page:{limit}:{before_id or 0}"

    cached = await redis_client.get(cache_key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass

    messages = await run_in_threadpool(get_messages_page_sync, limit, before_id)
    payload = await run_in_threadpool(serialize_messages_sync, messages)

    await redis_client.set(
        cache_key,
        json.dumps(payload, ensure_ascii=False),
        ex=MESSAGE_CACHE_TTL_SECONDS,
    )

    return payload


@app.post("/messages", status_code=status.HTTP_201_CREATED, response_model=MessageOut)
async def create_message_endpoint(
    text: str = Form(default=""),
    file: Optional[UploadFile] = File(default=None),
    auth: AuthContext = Depends(get_current_auth),
):
    user = auth.user

    has_upload = file is not None and bool(file.filename)

    if has_upload and (not s3_client or not S3_BUCKET):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Хранилище файлов не настроено",
        )

    await peek_rate_limit(user.id)

    file_data = None
    file_name = None
    file_content_type = None

    if has_upload:
        file_data = await read_upload_with_limit(file, MAX_FILE_SIZE)

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
    )


@app.get("/notifications")
async def get_notifications(auth: AuthContext = Depends(get_current_auth)):
    items = await redis_client.lrange("chat:notifications", 0, 49)

    result = []
    for item in items:
        try:
            result.append(json.loads(item))
        except Exception:
            continue

    return result


@app.websocket("/ws")
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)