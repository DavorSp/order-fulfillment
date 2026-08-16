import asyncio

import aio_pika
import asyncpg
from eventing import Envelope

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
QUEUE_NAME = "reserve_stock"



class StockRepository:
    # __init__ runs when you create a StockRepository. It receives the pool
    # and stores it on the object as self.pool, so every method can use it.
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool          # store the pool as this object's data

    # a method: operates on self.pool. Returns True if reserved, False if not.
    async def reserve(self, sku: str, qty: int) -> bool:
            async with self.pool.acquire() as connection:
                result = await connection.execute(
                    "UPDATE stock SET quantity = quantity - $1 "
                    "WHERE sku = $2 AND quantity >= $1",
                    qty, sku,
                )
                return result == "UPDATE 1"


# CHANGE 2: handle now receives the pool as an argument
async def handle(repo: StockRepository, envelope: Envelope) -> None:
    sku = envelope.payload.get("sku")
    qty = envelope.payload.get("qty")
    reserved = await repo.reserve(sku, qty)
    if reserved:
        print(f"Reserved {qty} of {sku}")
    else:
        print(f"Could not reserve {qty} of {sku} (not enough stock or no such SKU)")

async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DB_URL)
    repo = StockRepository(pool)          # <-- create the repository from the pool
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle(repo, envelope)   # <-- pass repo, not pool

if __name__ == "__main__":
    asyncio.run(main())