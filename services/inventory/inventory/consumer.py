import asyncio

import aio_pika
import asyncpg
from eventing import Envelope

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
QUEUE_NAME = "reserve_stock"

# CHANGE 1: removed the broken module-level pool line entirely


# CHANGE 2: handle now receives the pool as an argument
async def handle(pool: asyncpg.Pool, envelope: Envelope) -> None:
    sku = envelope.payload.get("sku")
    qty = envelope.payload.get("qty")

    async with pool.acquire() as connection:
        result = await connection.execute(
            "UPDATE stock SET quantity = quantity - $1 "
            "WHERE sku = $2 AND quantity >= $1",
            qty, sku,
        )
        if result == "UPDATE 1":
            print(f"Reserved {qty} of {sku}")
        else:
            print(f"Could not reserve {qty} of {sku} (not enough stock or no such SKU)")


async def main() -> None:
    # CHANGE 3: create the pool ONCE here, with await, after the loop is running
    pool = await asyncpg.create_pool(dsn=DB_URL)
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle(pool, envelope)  # pass the pool in


if __name__ == "__main__":
    asyncio.run(main())