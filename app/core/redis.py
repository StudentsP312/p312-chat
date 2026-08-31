import json
import uuid
import redis.asyncio as aioredis
from redis import Redis as SyncRedis
from app.core.config import settings
from app.core.exceptions import RateLimitExceededException, SpamDetectedException

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

# Asynchronous Redis client for FastAPI & async services
redis_client = aioredis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=3,
)

# Synchronous Redis client for Celery workers and synchronous tasks
redis_sync = SyncRedis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=5,
)

spam_script = redis_client.register_script(SPAM_LUA)
release_lock_script = redis_client.register_script(RELEASE_LOCK_LUA)


async def peek_rate_limit(user_id: int) -> None:
    """Pre-check if user is currently rate-limited without setting a new timestamp."""
    key = f"ratelimit:messages:{user_id}"
    exists = await redis_client.exists(key)

    if exists:
        pttl = await redis_client.pttl(key)
        retry_after = max(1, int((pttl + 999) / 1000)) if pttl and pttl > 0 else 1
        raise RateLimitExceededException(
            message="Слишком быстро. Одно сообщение в 5 секунд.",
            retry_after=retry_after,
        )


async def check_rate_limit(user_id: int) -> None:
    """Apply rate limiting check for a user."""
    key = f"ratelimit:messages:{user_id}"

    ok = await redis_client.set(key, "1", nx=True, px=settings.RATE_LIMIT_MS)
    if ok:
        return

    pttl = await redis_client.pttl(key)
    retry_after = max(1, int((pttl + 999) / 1000)) if pttl and pttl > 0 else 1
    raise RateLimitExceededException(
        message="Слишком быстро. Одно сообщение в 5 секунд.",
        retry_after=retry_after,
    )


async def check_spam(user_id: int, fingerprint: str) -> None:
    """Check consecutive duplicate messages using Lua script."""
    key = f"spam:{user_id}"

    allowed = await spam_script(
        keys=[key],
        args=[fingerprint, settings.MAX_CONSECUTIVE_SAME_MESSAGES, settings.SPAM_TTL_SECONDS],
    )

    if allowed != 1:
        raise SpamDetectedException(
            message="Нельзя отправлять одно и то же сообщение больше двух раз подряд."
        )


async def acquire_lock(name: str, timeout_ms: int = 15000) -> str | None:
    """Acquire a distributed lock with auto-expiration."""
    token = uuid.uuid4().hex
    ok = await redis_client.set(name, token, nx=True, px=timeout_ms)
    if ok:
        return token
    return None


async def release_lock(name: str, token: str) -> None:
    """Release a distributed lock safely using Lua script."""
    await release_lock_script(keys=[name], args=[token])


async def invalidate_message_cache() -> None:
    """Invalidate all cached message pages."""
    cursor = 0
    while True:
        cursor, keys = await redis_client.scan(cursor=cursor, match="messages:page:*", count=100)
        if keys:
            await redis_client.delete(*keys)
        if cursor == 0:
            break


async def publish_message(payload: dict) -> None:
    """Publish a chat message event to the Redis Pub/Sub channel."""
    await redis_client.publish("chat:messages", json.dumps(payload, ensure_ascii=False))
