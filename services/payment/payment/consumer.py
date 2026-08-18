import asyncio

import aio_pika
import asyncpg
from eventing import Envelope

from payment.broker import Broker
from payment.repository import PaymentRepository

AMQP_URL = "amqp://guest:guest@localhost/"
DB_URL = "postgresql://payment:payment@localhost:5433/payment"
QUEUE_NAME = "charge_payment"


async def handle(repo: PaymentRepository, broker: Broker, envelope: Envelope) -> None:
    order_id = envelope.payload.get("order_id")
    sku = envelope.payload.get("sku")
    qty = envelope.payload.get("qty")
    if order_id == "order-fail":
        await broker.publish_reply("PaymentFailed", order_id, sku, qty)
        print(f"Payment FAILED for {order_id}")
    else:
        await repo.charge(order_id)
        await broker.publish_reply("PaymentCharged", order_id, sku, qty)
        print(f"Payment CHARGED for {order_id}")


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DB_URL)
    repo = PaymentRepository(pool)
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        broker = Broker(channel)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle(repo, broker, envelope)


if __name__ == "__main__":
    asyncio.run(main())
