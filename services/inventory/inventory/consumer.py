import asyncio

import aio_pika
import asyncpg
from eventing import Envelope

from inventory.broker import Broker

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
QUEUE_NAME = "reserve_stock"  # <-- Inventory consumes ReserveStock commands here


class StockRepository:
    # __init__ runs when you create a StockRepository. It receives the pool
    # and stores it on the object as self.pool, so every method can use it.
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool  # store the pool as this object's data

    # a method: operates on self.pool. Returns True if reserved, False if not.
    async def reserve(self, sku: str, qty: int) -> bool:
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                "UPDATE stock SET quantity = quantity - $1 WHERE sku = $2 AND quantity >= $1",
                qty,
                sku,
            )
            return bool(result == "UPDATE 1")


async def handle(repo: StockRepository, broker: Broker, envelope: Envelope) -> None:
    order_id = envelope.payload["order_id"]
    sku = envelope.payload["sku"]
    qty = envelope.payload["qty"]
    assert isinstance(order_id, str)
    assert isinstance(sku, str)
    assert isinstance(qty, int)
    reserved = await repo.reserve(sku, qty)
    reply_type = "StockReserved" if reserved else "StockFailed"
    await broker.publish_reply(reply_type, order_id, sku, qty)
    print(f"{reply_type} for order {order_id}")


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DB_URL)
    repo = StockRepository(pool)
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        broker = Broker(channel)
        queue = await channel.declare_queue(
            QUEUE_NAME, durable=True
        )  # QUEUE_NAME = "reserve_stock"
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle(repo, broker, envelope)


if __name__ == "__main__":
    asyncio.run(main())
