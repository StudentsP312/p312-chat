import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
import jwt
from app.core.config import settings


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


def create_access_token(user_id: int, username: str) -> tuple[str, str, int]:
    jti = uuid.uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=settings.ACCESS_TOKEN_EXPIRE_SECONDS)

    payload = {
        "sub": str(user_id),
        "username": username,
        "jti": jti,
        "exp": expires_at,
    }

    token = jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.ALGORITHM)
    return token, jti, settings.ACCESS_TOKEN_EXPIRE_SECONDS
