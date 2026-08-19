import asyncio

import aio_pika
import asyncpg
from eventing import Envelope, Idempotency, constants
from redis.asyncio import Redis

from inventory.broker import Broker
from inventory.repository import StockRepository

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
REDIS_URL = "redis://localhost:6379"


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
        queue = await channel.declare_queue(constants.RESERVE_STOCK_QUEUE, durable=True)
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
