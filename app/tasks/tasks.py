import json
from datetime import datetime, timedelta, timezone
from celery.schedules import crontab
from redbeat import RedBeatSchedulerEntry
from app.core.config import settings
from app.core.database import SyncSessionLocal
from app.core.redis import redis_sync
from app.models.models import Message
from app.services.storage_service import StorageService
from app.tasks.celery_app import celery_app

storage_service = StorageService()


@celery_app.task(name="notify_message")
def notify_message(message_id: int) -> str:
    db = SyncSessionLocal()
    try:
        message = db.get(Message, message_id)
        if not message:
            return "not_found"

        payload = {
            "id": message.id,
            "username": message.username,
            "text": message.text or "",
            "has_file": message.has_file,
            "created_at": message.created_at.isoformat() if message.created_at else None,
        }

        redis_sync.lpush("chat:notifications", json.dumps(payload, ensure_ascii=False))
        redis_sync.ltrim("chat:notifications", 0, 999)
        return "ok"
    finally:
        db.close()


@celery_app.task(name="process_file_thumbnail")
def process_file_thumbnail(message_id: int) -> str:
    db = SyncSessionLocal()
    try:
        message = db.get(Message, message_id)
        if not message or not message.has_file:
            return "no_file"

        if not message.is_image:
            return "not_image"

        if not storage_service.is_available:
            return "s3_not_configured"

        thumbnail_key = storage_service.create_thumbnail_sync(
            file_key=message.file_key,  # type: ignore[arg-type]
            content_type=message.file_content_type,
        )

        if thumbnail_key:
            message.thumbnail_key = thumbnail_key
            db.commit()
            return "ok"

        return "skipped"
    finally:
        db.close()


@celery_app.task(name="cleanup_old_messages")
def cleanup_old_messages() -> int:
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

        if keys_to_delete:
            storage_service.delete_objects_sync(keys_to_delete)

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
def process_message(message_id: int) -> str:
    notify_message.delay(message_id)
    process_file_thumbnail.delay(message_id)
    return "queued"


def ensure_beat_schedule() -> None:
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
