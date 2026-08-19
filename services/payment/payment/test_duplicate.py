"""Test idempotency: send the SAME message twice (same message_id).
The first should process, the second should be skipped as a duplicate.
"""

import asyncio

import aio_pika
from eventing import Envelope

from payment.config import AMQP_URL

QUEUE_NAME = "charge_payment"


async def main() -> None:
    # Build ONE envelope — it has one fixed message_id.
    envelope = Envelope(
        type="ChargePayment",
        payload={"order_id": "dup-test", "sku": "WIDGET-1", "qty": 2},
    )
    print(f"[test] message_id = {envelope.message_id}")

    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(QUEUE_NAME, durable=True)

        # Publish the SAME envelope TWICE — same message_id both times.
        for i in (1, 2):
            await channel.default_exchange.publish(
                aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
                routing_key=QUEUE_NAME,
            )
            print(f"[test] sent copy {i}")


if __name__ == "__main__":
    asyncio.run(main())
