import io
import json
import os
from datetime import datetime, timedelta, timezone
from celery.schedules import crontab
from redbeat import RedBeatSchedulerEntry
from app.core.config import settings
from app.core.database import SyncSessionLocal
from app.core.redis import redis_sync
from app.core.s3 import s3_client
from app.models.models import Message
from app.tasks.celery_app import celery_app


@celery_app.task(name="notify_message")
def notify_message(message_id: int):
    db = SyncSessionLocal()
    try:
        message = db.get(Message, message_id)
        if not message:
            return "not_found"

        payload = {
            "id": message.id,
            "username": message.username,
            "text": message.text or "",
            "has_file": bool(message.file_key),
            "created_at": message.created_at.isoformat() if message.created_at else None,
        }

        redis_sync.lpush("chat:notifications", json.dumps(payload, ensure_ascii=False))
        redis_sync.ltrim("chat:notifications", 0, 999)
        return "ok"
    finally:
        db.close()


@celery_app.task(name="process_file_thumbnail")
def process_file_thumbnail(message_id: int):
    db = SyncSessionLocal()
    try:
        message = db.get(Message, message_id)
        if not message or not message.file_key:
            return "no_file"

        if not message.file_content_type or not message.file_content_type.startswith("image/"):
            return "not_image"

        if not s3_client or not settings.S3_BUCKET:
            return "s3_not_configured"

        try:
            from PIL import Image
        except Exception:
            return "pillow_not_installed"

        try:
            obj = s3_client.get_object(Bucket=settings.S3_BUCKET, Key=message.file_key)
            data = obj["Body"].read()

            image = Image.open(io.BytesIO(data))
            image.thumbnail((128, 128))

            fmt = (image.format or "PNG").upper()
            if fmt in ("JPEG", "JPG") and image.mode not in ("RGB", "L"):
                image = image.convert("RGB")

            buffer = io.BytesIO()
            image.save(buffer, format=fmt)

            thumbnail_key = f"thumbnails/{message.file_key}"
            s3_client.put_object(
                Bucket=settings.S3_BUCKET,
                Key=thumbnail_key,
                Body=buffer.getvalue(),
                ContentType=message.file_content_type,
            )

            message.thumbnail_key = thumbnail_key
            db.commit()
            return "ok"
        except Exception as exc:
            return f"error: {exc}"
    finally:
        db.close()


@celery_app.task(name="cleanup_old_messages")
def cleanup_old_messages():
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.MESSAGE_RETENTION_DAYS)

    db = SyncSessionLocal()
    try:
        old_messages = db.query(Message).filter(Message.created_at < cutoff).all()
        if not old_messages:
            return 0

        keys_to_delete = []

        for message in old_messages:
            if message.file_key:
                keys_to_delete.append(message.file_key)
            if message.thumbnail_key:
                keys_to_delete.append(message.thumbnail_key)
            db.delete(message)

        if keys_to_delete and s3_client and settings.S3_BUCKET:
            try:
                for i in range(0, len(keys_to_delete), 1000):
                    chunk = keys_to_delete[i:i + 1000]
                    s3_client.delete_objects(
                        Bucket=settings.S3_BUCKET,
                        Delete={"Objects": [{"Key": key} for key in chunk]},
                    )
            except Exception:
                pass

        db.commit()

        try:
            for key in redis_sync.scan_iter(match="messages:page:*"):
                redis_sync.delete(key)
        except Exception:
            pass

        return len(old_messages)
    finally:
        db.close()


@celery_app.task(name="process_message")
def process_message(message_id: int):
    notify_message.delay(message_id)
    process_file_thumbnail.delay(message_id)
    return "queued"


def ensure_beat_schedule():
    try:
        if redis_sync.exists("redbeat:bootstrap:v1"):
            return

        RedBeatSchedulerEntry(
            name="cleanup-old-messages-hourly",
            task="cleanup_old_messages",
            schedule=crontab(minute=0),
            app=celery_app,
        ).save()

        redis_sync.set("redbeat:bootstrap:v1", "1")
    except Exception:
        pass
