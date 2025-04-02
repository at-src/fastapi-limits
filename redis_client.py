import redis.asyncio as aioredis
from config import REDIS_HOST, REDIS_PORT, REDIS_DB

_client: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = await aioredis.from_url(
            f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
            decode_responses=True,
        )
    return _client


async def close_redis():
    global _client
    if _client:
        await _client.aclose()
        _client = None
