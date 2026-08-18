from redis.asyncio import Redis


class Idempotency:
    # Owns the Redis connection (its state), same as a repository owns its pool.
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def is_new(self, message_id: str) -> bool:
        """Atomically claim a message_id.

        Returns True if this is the FIRST time we've seen it (safe to process).
        Returns False if we've seen it before (a duplicate — skip it).
        """
        # Use SET with nx=True and an expiry (ex=3600 seconds = 1 hour).
        # redis.set(key, value, nx=True, ex=3600) returns True if it set the
        # key (new), None if the key already existed (duplicate).
        result = await self.redis.set(message_id, "processed", nx=True, ex=3600)
        return bool(result)
