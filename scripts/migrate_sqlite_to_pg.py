"""
Скрипт для одноразовой миграции данных из SQLite (chat.db) в PostgreSQL.

Использование:
    python scripts/migrate_sqlite_to_pg.py [--sqlite-path ./data/chat.db]
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from sqlalchemy import text
from app.core.config import settings
from app.core.database import SyncSessionLocal


def parse_args():
    parser = argparse.ArgumentParser(description="Миграция данных из SQLite в PostgreSQL")
    parser.add_argument(
        "--sqlite-path",
        default="./data/chat.db",
        help="Путь к файлу SQLite (по умолчанию: ./data/chat.db)",
    )
    return parser.parse_args()


def migrate_data(sqlite_path: str):
    if not os.path.exists(sqlite_path):
        print(f"[!] Файл SQLite '{sqlite_path}' не найден. Пропуск переноса данных.")
        return

    print(f"[*] Открытие SQLite базы данных: {sqlite_path}")
    conn_sqlite = sqlite3.connect(sqlite_path)
    conn_sqlite.row_factory = sqlite3.Row
    cursor_sqlite = conn_sqlite.cursor()

    pg_session = SyncSessionLocal()

    try:
        # 1. Перенос пользователей
        cursor_sqlite.execute("SELECT id, username, hashed_password, created_at FROM users ORDER BY id ASC")
        users = cursor_sqlite.fetchall()
        print(f"[*] Найдено пользователей в SQLite: {len(users)}")

        for u in users:
            pg_session.execute(
                text(
                    """
                    INSERT INTO users (id, username, hashed_password, created_at)
                    VALUES (:id, :username, :hashed_password, :created_at)
                    ON CONFLICT (id) DO UPDATE SET
                        username = EXCLUDED.username,
                        hashed_password = EXCLUDED.hashed_password
                    """
                ),
                {
                    "id": u["id"],
                    "username": u["username"],
                    "hashed_password": u["hashed_password"],
                    "created_at": u["created_at"],
                },
            )

        pg_session.commit()
        print("[+] Пользователи успешно перенесены.")

        # 2. Перенос сообщений
        cursor_sqlite.execute(
            """
            SELECT id, user_id, username, text, file_key, thumbnail_key, file_name, file_content_type, file_size, created_at
            FROM messages ORDER BY id ASC
            """
        )
        messages = cursor_sqlite.fetchall()
        print(f"[*] Найдено сообщений в SQLite: {len(messages)}")

        for m in messages:
            pg_session.execute(
                text(
                    """
                    INSERT INTO messages (id, user_id, username, text, file_key, thumbnail_key, file_name, file_content_type, file_size, created_at)
                    VALUES (:id, :user_id, :username, :text, :file_key, :thumbnail_key, :file_name, :file_content_type, :file_size, :created_at)
                    ON CONFLICT (id) DO NOTHING
                    """
                ),
                {
                    "id": m["id"],
                    "user_id": m["user_id"],
                    "username": m["username"],
                    "text": m["text"],
                    "file_key": m["file_key"],
                    "thumbnail_key": m["thumbnail_key"],
                    "file_name": m["file_name"],
                    "file_content_type": m["file_content_type"],
                    "file_size": m["file_size"],
                    "created_at": m["created_at"],
                },
            )

        pg_session.commit()
        print("[+] Сообщения успешно перенесены.")

        # 3. Синхронизация автоинкрементных sequence в PostgreSQL
        print("[*] Синхронизация PostgreSQL sequence...")
        pg_session.execute(
            text("SELECT setval(pg_get_serial_sequence('users', 'id'), COALESCE((SELECT MAX(id) FROM users), 1));")
        )
        pg_session.execute(
            text("SELECT setval(pg_get_serial_sequence('messages', 'id'), COALESCE((SELECT MAX(id) FROM messages), 1));")
        )
        pg_session.commit()
        print("[+] Sequence успешно обновлены. Миграция данных завершена!")

    except Exception as exc:
        pg_session.rollback()
        print(f"[x] Ошибка миграции: {exc}", file=sys.stderr)
        raise
    finally:
        cursor_sqlite.close()
        conn_sqlite.close()
        pg_session.close()


if __name__ == "__main__":
    args = parse_args()
    migrate_data(args.sqlite_path)
