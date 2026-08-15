import re
import secrets
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import (
    AuthContext,
    get_current_auth,
    get_user_by_username,
)
from app.core.config import settings
from app.core.database import get_db
from app.core.redis import acquire_lock, redis_client, release_lock
from app.core.security import (
    create_access_token,
    hash_password,
    verify_password,
)
from app.models.models import User
from app.schemas.schemas import (
    PasswordResetConfirm,
    PasswordResetRequest,
    Token,
    UserCreate,
    UserResponse,
)

router = APIRouter(prefix="/auth", tags=["auth"])


async def create_user(db: AsyncSession, username: str, password: str) -> User:
    result = await db.execute(select(User).where(User.username == username))
    exists = result.scalar_one_or_none()
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
    await db.commit()
    await db.refresh(user)
    return user


async def authenticate_user(db: AsyncSession, username: str, password: str) -> User | None:
    result = await db.execute(select(User).where(User.username == username))
    user = result.scalar_one_or_none()
    if not user or not verify_password(password, user.hashed_password):
        return None
    return user


async def update_password(db: AsyncSession, username: str, new_password: str) -> int | None:
    result = await db.execute(select(User).where(User.username == username))
    user = result.scalar_one_or_none()
    if not user:
        return None

    user.hashed_password = hash_password(new_password)
    await db.commit()
    return user.id


@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserResponse)
async def register(payload: UserCreate, db: AsyncSession = Depends(get_db)):
    username = payload.username.strip().lower()

    if not re.fullmatch(r"[a-z0-9_]{3,32}", username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Имя пользователя может содержать строчные латинские буквы, цифры и символ подчеркивания.",
        )

    user = await create_user(db, username, payload.password)
    return user


@router.post("/token", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
):
    username = form_data.username.strip().lower()

    user = await authenticate_user(db, username, form_data.password)
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


@router.post("/logout")
async def logout(auth: AuthContext = Depends(get_current_auth)):
    await redis_client.delete(f"session:{auth.jti}")
    await redis_client.srem(f"user_sessions:{auth.user.id}", auth.jti)
    return {"ok": True}


@router.post("/logout-all")
async def logout_all(auth: AuthContext = Depends(get_current_auth)):
    sessions = await redis_client.smembers(f"user_sessions:{auth.user.id}")

    if sessions:
        keys = [f"session:{jti}" for jti in sessions]
        await redis_client.delete(*keys)

    await redis_client.delete(f"user_sessions:{auth.user.id}")
    return {"ok": True}


@router.post("/password-reset/request")
async def request_password_reset(
    payload: PasswordResetRequest,
    db: AsyncSession = Depends(get_db),
):
    username = payload.username.strip().lower()

    rate_key = f"ratelimit:password_reset:{username}"
    ok = await redis_client.set(rate_key, "1", nx=True, px=60_000)

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Слишком много запросов сброса пароля",
        )

    user = await get_user_by_username(db, username)

    if user:
        code = f"{secrets.randbelow(1000000):06d}"
        await redis_client.set(f"reset:{username}", hash_password(code), ex=300)

        return {
            "detail": "Код сброса создан",
            "debug_code": code,
        }

    return {
        "detail": "Если пользователь существует, код сброса отправлен",
    }


@router.post("/password-reset/confirm")
async def confirm_password_reset(
    payload: PasswordResetConfirm,
    db: AsyncSession = Depends(get_db),
):
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

        user_id = await update_password(db, username, payload.new_password)
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
