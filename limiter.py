import time
from fastapi import Request, HTTPException, status
from redis_client import get_redis


class SlidingWindowLimiter:
    """
    Sliding window rate limiter backed by Redis sorted sets.
    Each member is a request timestamp; expired entries are pruned on every check.
    """

    def __init__(self, limit: int, window: int):
        self.limit = limit    # max requests
        self.window = window  # window size in seconds

    async def is_allowed(self, key: str) -> tuple[bool, int]:
        """
        Returns (allowed, remaining).
        key should uniquely identify the caller (user id, IP, api key, etc.)
        """
        r = await get_redis()
        now = time.time()
        window_start = now - self.window
        rkey = f"rl:{key}"

        # Check current count atomically before deciding to add
        async with r.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(rkey, 0, window_start)
            pipe.zcard(rkey)
            results = await pipe.execute()

        count: int = results[1]

        if count >= self.limit:
            return False, 0

        # Slot available — record this request
        await r.zadd(rkey, {str(now): now})
        await r.expire(rkey, self.window)
        return True, self.limit - count - 1

    def as_dependency(self, key_func=None):
        """
        Returns a FastAPI dependency.
        key_func(request) -> str   defaults to client IP
        """
        limiter = self

        async def dep(request: Request):
            key = key_func(request) if key_func else (request.client.host or "anon")
            allowed, remaining = await limiter.is_allowed(key)
            if not allowed:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="rate limit exceeded",
                    headers={"Retry-After": str(limiter.window)},
                )
            return remaining

        return dep


def rate_limit(limit: int = 60, window: int = 60, key_func=None):
    """Convenience wrapper — use as a FastAPI dependency."""
    return SlidingWindowLimiter(limit, window).as_dependency(key_func)
