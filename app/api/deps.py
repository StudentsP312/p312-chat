from dataclasses import dataclass
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.core.database import AsyncSessionLocal, get_db
from app.core.redis import redis_client
from app.models.models import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


@dataclass
class AuthContext:
    user: User
    jti: str


async def get_user_by_id(db: AsyncSession, user_id: int) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def get_user_by_username(db: AsyncSession, username: str) -> User | None:
    result = await db.execute(select(User).where(User.username == username))
    return result.scalar_one_or_none()


async def get_current_auth(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> AuthContext:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Не удалось проверить токен",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.ALGORITHM])
        user_id = int(payload.get("sub"))
        jti = payload.get("jti")
    except Exception:
        raise credentials_exception

    if not jti:
        raise credentials_exception

    session_exists = await redis_client.exists(f"session:{jti}")
    if not session_exists:
        raise credentials_exception

    user = await get_user_by_id(db, user_id)
    if user is None:
        raise credentials_exception

    return AuthContext(user=user, jti=jti)


def get_current_user(auth: AuthContext = Depends(get_current_auth)) -> User:
    return auth.user


async def authenticate_token(token: str, db: AsyncSession | None = None) -> User | None:
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.ALGORITHM])
        user_id = int(payload.get("sub"))
        jti = payload.get("jti")
    except Exception:
        return None

    if not jti:
        return None

    session_exists = await redis_client.exists(f"session:{jti}")
    if not session_exists:
        return None

    if db is not None:
        return await get_user_by_id(db, user_id)

    async with AsyncSessionLocal() as session:
        return await get_user_by_id(session, user_id)
