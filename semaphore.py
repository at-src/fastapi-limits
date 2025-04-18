import asyncio
import time
from redis_client import get_redis


class SemaphoreAcquireError(Exception):
    pass


class NamedSemaphore:
    """
    Distributed counting semaphore backed by Redis.

    Tracks holders in a sorted set (member=holder_id, score=expiry timestamp).
    Expired holders are evicted on every acquire so crashed processes don't
    permanently consume slots.
    """

    def __init__(self, name: str, limit: int, ttl: int = 300, retry_interval: float = 0.5, timeout: float = 30.0):
        self.key = f"sem:{name}"
        self.limit = limit
        self.ttl = ttl
        self.retry_interval = retry_interval
        self.timeout = timeout

    async def acquire(self, holder_id: str) -> bool:
        r = await get_redis()
        deadline = time.monotonic() + self.timeout

        while time.monotonic() < deadline:
            now = time.time()

            async with r.pipeline(transaction=True) as pipe:
                # evict expired holders
                pipe.zremrangebyscore(self.key, 0, now)
                pipe.zcard(self.key)
                results = await pipe.execute()

            count: int = results[1]
            if count < self.limit:
                expiry = now + self.ttl
                added = await r.zadd(self.key, {holder_id: expiry}, nx=True)
                if added:
                    await r.expireat(self.key, int(expiry) + 1)
                    return True

            await asyncio.sleep(self.retry_interval)

        return False

    async def release(self, holder_id: str):
        r = await get_redis()
        await r.zrem(self.key, holder_id)

    async def count(self) -> int:
        r = await get_redis()
        await r.zremrangebyscore(self.key, 0, time.time())
        return await r.zcard(self.key)

    def as_context(self, holder_id: str):
        sem = self

        class _Ctx:
            async def __aenter__(self_):
                ok = await sem.acquire(holder_id)
                if not ok:
                    raise SemaphoreAcquireError(f"semaphore '{sem.key}' full (limit={sem.limit})")
                return self_

            async def __aexit__(self_, *_):
                await sem.release(holder_id)

        return _Ctx()
