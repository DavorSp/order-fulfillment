import asyncio
import sys

import aio_pika
from eventing import Envelope

from order.broker import Broker

AMQP_URL = "amqp://guest:guest@localhost/"
REPLY_QUEUE = "order_replies"
order_id = sys.argv[1] if len(sys.argv) > 1 else "order-123"


async def handle_reply(broker: Broker, envelope: Envelope) -> None:
    reply_type = envelope.type
    order_id = envelope.payload["order_id"]
    assert isinstance(order_id, str)

    if reply_type == "StockReserved":
        sku = envelope.payload["sku"]
        qty = envelope.payload["qty"]
        assert isinstance(sku, str)
        assert isinstance(qty, int)
        # stock is secured — proceed to charge payment
        print(f"Order {order_id}: stock reserved, charging payment")
        await broker.publish_charge_payment(order_id, sku, qty)

    elif reply_type == "StockFailed":
        # nothing was reserved, order simply can't proceed
        print(f"Order {order_id}: stock failed, order cannot proceed")

    elif reply_type == "PaymentCharged":
        # happy path complete
        print(f"Order {order_id}: payment charged, ORDER CONFIRMED")

    elif reply_type == "PaymentFailed":
        # payment failed AFTER stock was reserved — stock is now stuck.
        # TODO: compensation — release the reserved stock (next major step)
        print(f"Order {order_id}: payment failed, order cannot proceed (stock needs releasing!)")

    else:
        print(f"Order {order_id}: unknown reply type {reply_type}")


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        broker = Broker(channel)
        queue = await channel.declare_queue(REPLY_QUEUE, durable=True)

        await broker.publish_reserve_stock(order_id, "WIDGET-1", 2)

        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle_reply(broker, envelope)


if __name__ == "__main__":
    asyncio.run(main())
