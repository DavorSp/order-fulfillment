import asyncio

import aio_pika
import asyncpg
from eventing import Envelope

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://inventory:inventory@localhost:5432/inventory"
QUEUE_NAME = "reserve_stock"

async def handle(envelope: Envelope) -> None:
    sku = envelope.payload.get("sku")
    qty = envelope.payload.get("qty")
    async with asyncpg.create_pool(dsn=DB_URL) as pool:
        async with pool.acquire() as connection:
            current_quantity = await connection.fetchval(
                "SELECT quantity FROM stock WHERE sku = $1", sku
            )
            if current_quantity is None:
                print(f"No such SKU: {sku}")
                return
            if current_quantity >= qty:
                new_quantity = current_quantity - qty
                await connection.execute(
                    "UPDATE stock SET quantity = $1 WHERE sku = $2", new_quantity, sku
                )
                print(f"Reserved {qty} of {sku}, new quantity is {new_quantity}")
            else:
                print(f"Not enough stock for {sku}. Requested {qty}, available {current_quantity}")

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