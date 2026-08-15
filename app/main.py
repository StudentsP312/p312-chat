import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from app.api.v1.auth import router as auth_router
from app.api.v1.health import router as health_router
from app.api.v1.messages import router as messages_router
from app.api.v1.websocket import redis_listener, router as websocket_router
from app.core.redis import redis_client
from app.tasks.tasks import ensure_beat_schedule

STATIC_DIR = Path(__file__).resolve().parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(ensure_beat_schedule)
    listener_task = asyncio.create_task(redis_listener())
    yield
    listener_task.cancel()

    try:
        await listener_task
    except asyncio.CancelledError:
        pass

    try:
        await redis_client.aclose()
    except Exception:
        pass


app = FastAPI(title="Public chat API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files mounting
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=FileResponse, include_in_schema=False)
async def get_index():
    index_file = STATIC_DIR / "index.html"
    return FileResponse(index_file)


app.include_router(health_router)
app.include_router(auth_router)
app.include_router(messages_router)
app.include_router(websocket_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

