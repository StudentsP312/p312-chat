import boto3
from fastapi import HTTPException, status
from app.core.config import settings

s3_client = None
if settings.S3_BUCKET:
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        region_name=settings.S3_REGION_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )


def build_object_url(key: str | None) -> str | None:
    if not key:
        return None

    if settings.S3_PUBLIC_BASE_URL:
        return f"{settings.S3_PUBLIC_BASE_URL.rstrip('/')}/{key}"

    if not s3_client or not settings.S3_BUCKET:
        return None

    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": settings.S3_BUCKET,
                "Key": key,
            },
            ExpiresIn=3600,
        )
    except Exception:
        return None


def upload_to_s3(key: str, data: bytes, content_type: str) -> None:
    if not s3_client or not settings.S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Хранилище файлов не настроено",
        )

    s3_client.put_object(
        Bucket=settings.S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def check_s3_sync() -> None:
    if s3_client and settings.S3_BUCKET:
        s3_client.head_bucket(Bucket=settings.S3_BUCKET)
