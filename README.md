# Chat API

Public chat with auth, file uploads, WebSocket, rate limiting and background tasks.

## Requirements

- Python 3.13
- Redis
- PostgreSQL or SQLite
- S3-compatible storage

## Install

```bash
uv init --name chat-api --python 3.13
uv add fastapi uvicorn sqlalchemy python-multipart boto3 pyjwt redis celery pillow celery-redbeat
```

## Configure

Copy `.env.example` to `.env` and set values.

## Run

API:
```bash
uv run uvicorn app:app --host 0.0.0.0 --port 8000
```

Worker:
```bash
uv run celery -A app.celery_app worker -l info
```

Beat (safe on multiple machines):
```bash
uv run celery -A app.celery_app beat -l info
```

## Endpoints

- `POST /auth/register`
- `POST /auth/token`
- `POST /auth/logout`
- `POST /auth/logout-all`
- `POST /auth/password-reset/request`
- `POST /auth/password-reset/confirm`
- `GET /messages`
- `POST /messages`
- `GET /notifications`
- `GET /health`
- `WS /ws?token=JWT`

## Notes

- Beat uses Redis lock via celery-redbeat, can run on all nodes.
- Health checks DB, Redis and optionally S3 (`HEALTH_CHECK_S3=true`).
- For production use PostgreSQL and external Redis.