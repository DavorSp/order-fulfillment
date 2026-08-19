"""Throwaway test harness: pretend to be Order, send Payment one ChargePayment.

Usage:
    uv run python -m payment.test_publish            # charges (order-123)
    uv run python -m payment.test_publish order-fail # fails
"""

import asyncio
import sys

import aio_pika
from eventing import Envelope, constants

AMQP_URL = "amqp://guest:guest@localhost/"


async def main() -> None:
    order_id = sys.argv[1] if len(sys.argv) > 1 else "order-123"
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(constants.CHARGE_PAYMENT_QUEUE, durable=True)
        envelope = Envelope(
            type="ChargePayment",
            payload={"order_id": order_id, "sku": "WIDGET-1", "qty": 2},
        )
        await channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key=constants.ORDER_REPLIES_QUEUE,
        )
        print(f"[test] sent ChargePayment for {order_id}")


if __name__ == "__main__":
    asyncio.run(main())
