import asyncio
import uuid
import time
from redis_client import get_redis


class LockAcquireError(Exception):
    pass


class DistributedLock:
    """
    Async context manager for a Redis-backed distributed lock.

    Uses SET NX PX with a random token so only the owner can release it.
    Optionally retries until timeout is exceeded.
    """

    def __init__(self, name: str, ttl: int = 30, retry_interval: float = 0.1, timeout: float = 10.0):
        self.name = f"lock:{name}"
        self.ttl = ttl                      # seconds before auto-expiry
        self.retry_interval = retry_interval
        self.timeout = timeout
        self._token: str | None = None

    async def acquire(self) -> bool:
        r = await get_redis()
        self._token = uuid.uuid4().hex
        deadline = time.monotonic() + self.timeout

        while time.monotonic() < deadline:
            ok = await r.set(self.name, self._token, nx=True, ex=self.ttl)
            if ok:
                return True
            await asyncio.sleep(self.retry_interval)

        return False

    async def release(self):
        r = await get_redis()
        # Lua script: only delete if token matches (atomic)
        script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        await r.eval(script, 1, self.name, self._token)

    async def __aenter__(self):
        acquired = await self.acquire()
        if not acquired:
            raise LockAcquireError(f"could not acquire lock '{self.name}' within {self.timeout}s")
        return self

    async def __aexit__(self, *_):
        await self.release()
