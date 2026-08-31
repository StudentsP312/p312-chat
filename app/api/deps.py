from dataclasses import dataclass
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import AsyncSessionLocal, get_db
from app.core.exceptions import AppException
from app.models.models import User
from app.services.auth_service import AuthService
from app.services.health_service import HealthService
from app.services.message_service import MessageService
from app.services.storage_service import StorageService

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


@dataclass
class AuthContext:
    user: User
    jti: str


def get_storage_service() -> StorageService:
    """Dependency for obtaining StorageService singleton/instance."""
    return StorageService()


def get_auth_service(db: AsyncSession = Depends(get_db)) -> AuthService:
    """Dependency for obtaining AuthService with current database session."""
    return AuthService(db)


def get_message_service(
    db: AsyncSession = Depends(get_db),
    storage: StorageService = Depends(get_storage_service),
) -> MessageService:
    """Dependency for obtaining MessageService."""
    return MessageService(db, storage)


def get_health_service(
    db: AsyncSession = Depends(get_db),
    storage: StorageService = Depends(get_storage_service),
) -> HealthService:
    """Dependency for obtaining HealthService."""
    return HealthService(db, storage)


async def get_current_auth(
    token: str = Depends(oauth2_scheme),
    auth_service: AuthService = Depends(get_auth_service),
) -> AuthContext:
    """Validate Bearer token and active session, returning authenticated context."""
    try:
        user, jti = await auth_service.validate_session(token)
        return AuthContext(user=user, jti=jti)
    except AppException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.message,
            headers=exc.headers,
        ) from exc


def get_current_user(auth: AuthContext = Depends(get_current_auth)) -> User:
    """Dependency shortcut to extract User from AuthContext."""
    return auth.user


async def authenticate_token(token: str, db: AsyncSession | None = None) -> User | None:
    """Compatibility helper to authenticate a raw token."""
    if db is not None:
        auth_service = AuthService(db)
        try:
            user, _ = await auth_service.validate_session(token)
            return user
        except Exception:
            return None

    async with AsyncSessionLocal() as session:
        auth_service = AuthService(session)
        try:
            user, _ = await auth_service.validate_session(token)
            return user
        except Exception:
            return None
