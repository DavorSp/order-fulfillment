"""Step 1: prove the infrastructure works.

Publish one ReserveStock message to a queue, then consume it and print it.
One service, one message, one round trip. If this runs, RabbitMQ is reachable
and the envelope survives the trip. Everything else in the project is a
variation on this.
"""

from __future__ import annotations

import asyncio
import os

import aio_pika
from eventing import Envelope

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost/")
QUEUE_NAME = "reserve_stock"


async def publish(channel: aio_pika.abc.AbstractChannel) -> None:
    envelope = Envelope(
        type="ReserveStock",
        payload={"order_id": "order-123", "sku": "WIDGET-1", "qty": 2},
    )
    await channel.default_exchange.publish(
        aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
        routing_key=QUEUE_NAME,
    )
    print(f"[publish] sent {envelope.type} id={envelope.message_id}")


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(QUEUE_NAME, durable=True)
        await publish(channel)
        print("[done] published ReserveStock")


if __name__ == "__main__":
    asyncio.run(main())