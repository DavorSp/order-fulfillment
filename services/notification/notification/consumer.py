import asyncio

import aio_pika
from eventing import Envelope

AMQP_URL = "amqp://guest:guest@localhost/"
QUEUE_NAME = "notifications"


async def handle(envelope: Envelope) -> None:
    order_id = envelope.payload["order_id"]
    if envelope.type == "OrderConfirmed":
        print(f"Notifying customer: order {order_id} CONFIRMED")
    elif envelope.type == "OrderFailed":
        print(f"Notifying customer: order {order_id} FAILED")


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle(envelope)


if __name__ == "__main__":
    asyncio.run(main())
