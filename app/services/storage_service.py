import io
import os
import re
import uuid
from typing import BinaryIO
from fastapi.concurrency import run_in_threadpool
from app.core.config import settings
from app.core.exceptions import StorageUnavailableException
from app.core.s3 import build_object_url, check_s3_sync, is_storage_configured, s3_client, upload_to_s3


class StorageService:
    """Service encapsulating object storage (S3/MinIO) operations and file processing."""

    def __init__(self) -> None:
        self.bucket = settings.S3_BUCKET
        self.public_base_url = settings.S3_PUBLIC_BASE_URL

    @property
    def is_available(self) -> bool:
        """Check if S3 storage is configured and available."""
        return is_storage_configured()

    def generate_file_key(self, filename: str | None, prefix: str = "chat") -> str:
        """Generate a safe, unique object key preserving safe extension."""
        raw_ext = os.path.splitext(filename or "")[1].lower()
        file_ext = re.sub(r"[^a-z0-9.]", "", raw_ext)[:20]
        return f"{prefix}/{uuid.uuid4().hex}{file_ext}"

    def get_url(self, key: str | None) -> str | None:
        """Get public or presigned URL for an object key."""
        return build_object_url(key)

    async def upload(self, key: str, data: bytes, content_type: str) -> None:
        """Upload raw data to S3 asynchronously using threadpool."""
        if not self.is_available:
            raise StorageUnavailableException("Хранилище файлов не настроено")

        await run_in_threadpool(upload_to_s3, key, data, content_type)

    def upload_sync(self, key: str, data: bytes, content_type: str) -> None:
        """Upload raw data to S3 synchronously."""
        if not self.is_available:
            raise StorageUnavailableException("Хранилище файлов не настроено")

        upload_to_s3(key, data, content_type)

    def delete_objects_sync(self, keys: list[str]) -> None:
        """Delete multiple objects from S3 in batches synchronously."""
        if not keys or not self.is_available or not s3_client:
            return

        try:
            for i in range(0, len(keys), 1000):
                chunk = keys[i : i + 1000]
                s3_client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": [{"Key": key} for key in chunk]},
                )
        except Exception:
            pass

    def create_thumbnail_sync(
        self,
        file_key: str,
        content_type: str | None,
        max_size: tuple[int, int] = (128, 128),
    ) -> str | None:
        """Fetch image from S3, generate thumbnail using Pillow, upload and return thumbnail key."""
        if not self.is_available or not s3_client or not file_key:
            return None

        if not content_type or not content_type.startswith("image/"):
            return None

        try:
            from PIL import Image
        except ImportError:
            return None

        try:
            obj = s3_client.get_object(Bucket=self.bucket, Key=file_key)
            data = obj["Body"].read()

            image = Image.open(io.BytesIO(data))
            image.thumbnail(max_size)

            fmt = (image.format or "PNG").upper()
            if fmt in ("JPEG", "JPG") and image.mode not in ("RGB", "L"):
                image = image.convert("RGB")

            buffer = io.BytesIO()
            image.save(buffer, format=fmt)

            thumbnail_key = f"thumbnails/{file_key}"
            s3_client.put_object(
                Bucket=self.bucket,
                Key=thumbnail_key,
                Body=buffer.getvalue(),
                ContentType=content_type,
            )
            return thumbnail_key
        except Exception:
            return None

    def check_health_sync(self) -> None:
        """Verify bucket access synchronously."""
        check_s3_sync()
