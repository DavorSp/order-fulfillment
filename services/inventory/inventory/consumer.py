import asyncio

import aio_pika
import asyncpg
from eventing import Envelope, Idempotency
from redis.asyncio import Redis

from inventory.broker import Broker

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
QUEUE_NAME = "reserve_stock"
REDIS_URL = "redis://localhost:6379"


class StockRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def reserve(self, sku: str, qty: int) -> bool:
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                "UPDATE stock SET quantity = quantity - $1 WHERE sku = $2 AND quantity >= $1",
                qty,
                sku,
            )
            return bool(result == "UPDATE 1")

    async def release(self, sku: str, qty: int) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE stock SET quantity = quantity + $1 WHERE sku = $2",
                qty,
                sku,
            )


async def handle(repo: StockRepository, broker: Broker, envelope: Envelope) -> None:
    order_id = envelope.payload["order_id"]
    sku = envelope.payload["sku"]
    qty = envelope.payload["qty"]

    if envelope.type == "ReserveStock":
        reserved = await repo.reserve(sku, qty)
        reply_type = "StockReserved" if reserved else "StockFailed"
        await broker.publish_reply(reply_type, order_id, sku, qty)
        print(f"{reply_type} for order {order_id}")

    elif envelope.type == "ReleaseStock":
        await repo.release(sku, qty)
        print(f"Released {qty} of {sku} for order {order_id}")


async def main() -> None:
    redis = Redis.from_url(REDIS_URL)
    idempotency = Idempotency(redis)
    pool = await asyncpg.create_pool(dsn=DB_URL)
    repo = StockRepository(pool)
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        broker = Broker(channel)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    if not await idempotency.is_new(envelope.message_id):
                        print(f"Skipping duplicate {envelope.message_id}")
                        continue
                    await handle(repo, broker, envelope)


if __name__ == "__main__":
    asyncio.run(main())
