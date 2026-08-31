from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.api.deps import AuthContext, get_auth_service, get_current_auth
from app.core.exceptions import AppException
from app.schemas.schemas import (
    PasswordResetConfirm,
    PasswordResetRequest,
    Token,
    UserCreate,
    UserResponse,
)
from app.services.auth_service import AuthService

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=UserResponse)
async def register(
    payload: UserCreate,
    auth_service: AuthService = Depends(get_auth_service),
):
    try:
        user = await auth_service.register(payload.username, payload.password)
        return user
    except AppException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.message,
            headers=exc.headers,
        ) from exc


@router.post("/token", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    auth_service: AuthService = Depends(get_auth_service),
):
    try:
        _, token, _, _ = await auth_service.authenticate(form_data.username, form_data.password)
        return Token(access_token=token)
    except AppException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.message,
            headers=exc.headers,
        ) from exc


@router.post("/logout")
async def logout(
    auth: AuthContext = Depends(get_current_auth),
    auth_service: AuthService = Depends(get_auth_service),
):
    await auth_service.logout(auth.user.id, auth.jti)
    return {"ok": True}


@router.post("/logout-all")
async def logout_all(
    auth: AuthContext = Depends(get_current_auth),
    auth_service: AuthService = Depends(get_auth_service),
):
    await auth_service.logout_all(auth.user.id)
    return {"ok": True}


@router.post("/password-reset/request")
async def request_password_reset(
    payload: PasswordResetRequest,
    auth_service: AuthService = Depends(get_auth_service),
):
    try:
        found, code = await auth_service.request_password_reset(payload.username)
        if found:
            return {
                "detail": "Код сброса создан",
                "debug_code": code,
            }
        return {
            "detail": "Если пользователь существует, код сброса отправлен",
        }
    except AppException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.message,
            headers=exc.headers,
        ) from exc


@router.post("/password-reset/confirm")
async def confirm_password_reset(
    payload: PasswordResetConfirm,
    auth_service: AuthService = Depends(get_auth_service),
):
    try:
        await auth_service.confirm_password_reset(
            username=payload.username,
            code=payload.code,
            new_password=payload.new_password,
        )
        return {"ok": True}
    except AppException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.message,
            headers=exc.headers,
        ) from exc
