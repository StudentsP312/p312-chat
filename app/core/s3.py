import boto3
from app.core.config import settings
from app.core.exceptions import StorageUnavailableException

s3_client = None
if settings.S3_BUCKET:
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT_URL,
        region_name=settings.S3_REGION_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )


def is_storage_configured() -> bool:
    """Check if S3 storage client and bucket are configured."""
    return bool(s3_client and settings.S3_BUCKET)


def build_object_url(key: str | None) -> str | None:
    """Build public or presigned URL for an object key."""
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
    """Upload raw byte data to configured S3 bucket."""
    if not is_storage_configured():
        raise StorageUnavailableException("Хранилище файлов не настроено")

    s3_client.put_object(  # type: ignore[union-attr]
        Bucket=settings.S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def check_s3_sync() -> None:
    """Synchronous head bucket check for health monitoring."""
    if is_storage_configured():
        s3_client.head_bucket(Bucket=settings.S3_BUCKET)  # type: ignore[union-attr]
