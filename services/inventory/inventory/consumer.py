import asyncio

import aio_pika
import asyncpg
from eventing import Envelope

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
QUEUE_NAME = "reserve_stock"

async def handle(envelope: Envelope) -> None:
    # 1. get requested sku and qty from envelope.payload
    sku= envelope.payload.get("sku")
    qty = envelope.payload.get("qty")
    # 2. connect to the DB, fetchval the current quantity for that sku
    async with asyncpg.create_pool(dsn=DB_URL) as pool:
        async with pool.acquire() as connection:
            current_quantity = await connection.fetchval(
                "SELECT quantity FROM stock WHERE sku = $1", sku
            )
    # 3. print: requested X of <sku>, have Y in stock
    print(f"Requested {qty} of {sku}, have {current_quantity} in stock")


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():  # acks on success, requeues on error
                    envelope = Envelope.from_bytes(message.body)
                    await handle(envelope)

if __name__ == "__main__":
    asyncio.run(main())