import re
import secrets
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.exceptions import (
    AppException,
    InvalidCredentialsException,
    InvalidTokenException,
    LockAcquisitionException,
    PasswordResetException,
    UserAlreadyExistsException,
    UserNotFoundException,
)
from app.core.redis import acquire_lock, redis_client, release_lock
from app.core.security import create_access_token, decode_access_token, hash_password, verify_password
from app.models.models import User

USERNAME_PATTERN = re.compile(r"^[a-z0-9_]{3,32}$")


class AuthService:
    """Rich service encapsulating user identity, authentication, password reset, and session lifecycles."""

    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def get_user_by_id(self, user_id: int) -> User | None:
        """Find user by unique primary key."""
        result = await self.db.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def get_user_by_username(self, username: str) -> User | None:
        """Find user by unique username."""
        normalized = username.strip().lower()
        result = await self.db.execute(select(User).where(User.username == normalized))
        return result.scalar_one_or_none()

    async def register(self, username: str, password: str) -> User:
        """Register a new user with format and uniqueness validation."""
        normalized = username.strip().lower()

        if not USERNAME_PATTERN.fullmatch(normalized):
            raise AppException(
                message="Имя пользователя может содержать строчные латинские буквы, цифры и символ подчеркивания.",
                status_code=400,
            )

        existing = await self.get_user_by_username(normalized)
        if existing:
            raise UserAlreadyExistsException("Имя пользователя уже занято")

        user = User.create(username=normalized, raw_password=password)
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        return user

    async def authenticate(self, username: str, password: str) -> tuple[User, str, str, int]:
        """Authenticate user credentials and issue a new tracked session."""
        normalized = username.strip().lower()
        user = await self.get_user_by_username(normalized)

        if not user or not user.check_password(password):
            raise InvalidCredentialsException()

        token, jti, ttl = create_access_token(user.id, user.username)

        # Register session in Redis for instant invalidation support
        await redis_client.set(f"session:{jti}", str(user.id), ex=ttl)
        await redis_client.sadd(f"user_sessions:{user.id}", jti)
        await redis_client.expire(f"user_sessions:{user.id}", ttl)

        return user, token, jti, ttl

    async def validate_session(self, token: str) -> tuple[User, str]:
        """Validate JWT and active session in Redis, returning User and session jti."""
        payload = decode_access_token(token)
        user_id_raw = payload.get("sub")
        jti = payload.get("jti")

        if not user_id_raw or not jti:
            raise InvalidTokenException()

        user_id = int(user_id_raw)
        session_exists = await redis_client.exists(f"session:{jti}")
        if not session_exists:
            raise InvalidTokenException()

        user = await self.get_user_by_id(user_id)
        if user is None:
            raise InvalidTokenException()

        return user, jti

    async def logout(self, user_id: int, jti: str) -> None:
        """Revoke a single user session."""
        await redis_client.delete(f"session:{jti}")
        await redis_client.srem(f"user_sessions:{user_id}", jti)

    async def logout_all(self, user_id: int) -> None:
        """Revoke all active sessions for the user."""
        sessions = await redis_client.smembers(f"user_sessions:{user_id}")
        if sessions:
            keys = [f"session:{jti}" for jti in sessions]
            await redis_client.delete(*keys)
        await redis_client.delete(f"user_sessions:{user_id}")

    async def request_password_reset(self, username: str) -> tuple[bool, str | None]:
        """Initiate password reset workflow with rate limiting."""
        normalized = username.strip().lower()

        rate_key = f"ratelimit:password_reset:{normalized}"
        ok = await redis_client.set(rate_key, "1", nx=True, px=60_000)
        if not ok:
            raise PasswordResetException("Слишком много запросов сброса пароля", status_code=429)

        user = await self.get_user_by_username(normalized)
        if not user:
            return False, None

        code = f"{secrets.randbelow(1000000):06d}"
        await redis_client.set(f"reset:{normalized}", hash_password(code), ex=300)
        return True, code

    async def confirm_password_reset(self, username: str, code: str, new_password: str) -> None:
        """Confirm password reset code under distributed lock and update user password."""
        normalized = username.strip().lower()
        lock_name = f"lock:password_reset:{normalized}"
        lock_token = await acquire_lock(lock_name, timeout_ms=10000)

        if not lock_token:
            raise LockAcquisitionException("Слишком много одновременных запросов сброса")

        try:
            attempts_key = f"reset_attempts:{normalized}"
            attempts = await redis_client.incr(attempts_key)
            await redis_client.expire(attempts_key, 300)

            if attempts > 5:
                raise PasswordResetException("Слишком много попыток подтверждения кода", status_code=429)

            stored = await redis_client.get(f"reset:{normalized}")
            if not stored or not verify_password(code.strip(), stored):
                raise PasswordResetException("Неверный код сброса", status_code=400)

            user = await self.get_user_by_username(normalized)
            if not user:
                raise UserNotFoundException()

            user.set_password(new_password)
            await self.db.commit()

            # Clean up reset tokens
            await redis_client.delete(f"reset:{normalized}")
            await redis_client.delete(attempts_key)

            # Invalidate all user sessions upon password change
            await self.logout_all(user.id)
        finally:
            await release_lock(lock_name, lock_token)
