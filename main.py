import uuid
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel

from redis_client import get_redis, close_redis
from limiter import rate_limit
from lock import DistributedLock, LockAcquireError
from semaphore import NamedSemaphore, SemaphoreAcquireError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_redis()
    yield
    await close_redis()


app = FastAPI(lifespan=lifespan)

# --- rate limiter ---

@app.get("/limited", dependencies=[Depends(rate_limit(limit=10, window=60))])
async def limited_endpoint():
    return {"ok": True}


# --- distributed lock ---

class LockRequest(BaseModel):
    name: str
    ttl: int = 30

@app.post("/lock/run")
async def run_with_lock(body: LockRequest):
    """Acquire a named lock, do work, release. 409 if lock is held."""
    try:
        async with DistributedLock(body.name, ttl=body.ttl, timeout=0.1):
            # placeholder for work that must not run concurrently
            return {"acquired": True, "lock": body.name}
    except LockAcquireError:
        raise HTTPException(status_code=409, detail=f"lock '{body.name}' is held")


# --- semaphore ---

_sem = NamedSemaphore("workers", limit=5, ttl=120)

class SemRequest(BaseModel):
    holder_id: str | None = None

@app.post("/semaphore/acquire")
async def acquire_slot(body: SemRequest):
    holder = body.holder_id or uuid.uuid4().hex
    ok = await _sem.acquire(holder)
    if not ok:
        raise HTTPException(status_code=503, detail="no slots available")
    return {"holder_id": holder, "active": await _sem.count()}

@app.post("/semaphore/release/{holder_id}")
async def release_slot(holder_id: str):
    await _sem.release(holder_id)
    return {"holder_id": holder_id, "active": await _sem.count()}

@app.get("/semaphore/status")
async def semaphore_status():
    return {"active": await _sem.count(), "limit": _sem.limit}


# --- health ---

@app.get("/health")
async def health():
    r = await get_redis()
    await r.ping()
    return {"status": "ok"}
