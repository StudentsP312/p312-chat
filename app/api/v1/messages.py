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
from app.api.deps import AuthContext, get_current_auth, get_message_service
from app.core.exceptions import AppException
from app.schemas.schemas import MessageOut
from app.services.message_service import MessageService

router = APIRouter(tags=["messages"])


@router.get("/messages", response_model=list[MessageOut])
async def get_messages(
    limit: int = Query(default=50, ge=1, le=100),
    before_id: int | None = Query(default=None),
    auth: AuthContext = Depends(get_current_auth),
    message_service: MessageService = Depends(get_message_service),
):
    return await message_service.get_messages_page(limit=limit, before_id=before_id)


@router.post("/messages", status_code=status.HTTP_201_CREATED, response_model=MessageOut)
async def create_message_endpoint(
    text: str = Form(default=""),
    file: UploadFile | None = File(default=None),
    auth: AuthContext = Depends(get_current_auth),
    message_service: MessageService = Depends(get_message_service),
):
    user = auth.user
    has_upload = file is not None and bool(file.filename)

    try:
        if has_upload:
            if not message_service.storage.is_available:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Хранилище файлов не настроено",
                )

        await message_service.precheck_rate_limit(user.id)

        file_data = None
        file_name = None
        file_content_type = None

        if has_upload and file is not None:
            file_data = await message_service.read_upload(file)
            if len(file_data) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Файл пустой",
                )
            file_name = file.filename
            file_content_type = file.content_type or "application/octet-stream"

        return await message_service.create_message(
            user=user,
            text=text,
            file_data=file_data,
            file_name=file_name,
            file_content_type=file_content_type,
        )
    except AppException as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.message,
            headers=exc.headers,
        ) from exc


@router.get("/notifications")
async def get_notifications(
    auth: AuthContext = Depends(get_current_auth),
    message_service: MessageService = Depends(get_message_service),
):
    return await message_service.get_notifications()
