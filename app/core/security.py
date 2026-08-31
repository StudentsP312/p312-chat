import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
import jwt
from passlib.context import CryptContext
from app.core.config import settings
from app.core.exceptions import InvalidTokenException

# Standard password context with Argon2
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


def hash_password(password: str) -> str:
    """Hash password using Argon2 via standard pwd_context."""
    return pwd_context.hash(password)


def verify_password(password: str, hashed_password: str) -> bool:
    """Verify password against stored Argon2 hash."""
    try:
        return pwd_context.verify(password, hashed_password)
    except Exception:
        return False


def create_access_token(user_id: int, username: str) -> tuple[str, str, int]:
    """Generate a signed JWT token, returning (token, jti, ttl_seconds)."""
    jti = uuid.uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=settings.ACCESS_TOKEN_EXPIRE_SECONDS)

    payload: dict[str, Any] = {
        "sub": str(user_id),
        "username": username,
        "jti": jti,
        "exp": expires_at,
    }

    token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.ALGORITHM)
    return token, jti, settings.ACCESS_TOKEN_EXPIRE_SECONDS


def decode_access_token(token: str) -> dict[str, Any]:
    """Decode and validate a JWT access token."""
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.ALGORITHM])
    except Exception as exc:
        raise InvalidTokenException("Не удалось проверить токен") from exc
