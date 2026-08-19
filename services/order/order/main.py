import asyncio
import sys

import aio_pika
from eventing import Envelope, constants

from order.broker import Broker

AMQP_URL = "amqp://guest:guest@localhost/"
order_id = sys.argv[1] if len(sys.argv) > 1 else "order-123"


async def handle_reply(broker: Broker, envelope: Envelope) -> None:
    reply_type = envelope.type
    order_id = envelope.payload["order_id"]
    sku = envelope.payload["sku"]
    qty = envelope.payload["qty"]

    if reply_type == "StockReserved":
        print(f"Order {order_id}: stock reserved, charging payment")
        await broker.publish_charge_payment(order_id, sku, qty)

    elif reply_type == "StockFailed":
        print(f"Order {order_id}: stock failed, order cannot proceed")
        await broker.publish_notification("OrderFailed", order_id, sku, qty)

    elif reply_type == "PaymentCharged":
        print(f"Order {order_id}: payment charged, ORDER CONFIRMED")
        await broker.publish_notification("OrderConfirmed", order_id, sku, qty)

    elif reply_type == "PaymentFailed":
        print(f"Order {order_id}: payment failed, releasing stock")
        await broker.publish_release_stock(order_id, sku, qty)
        await broker.publish_notification("OrderFailed", order_id, sku, qty)

    else:
        print(f"Order {order_id}: unknown reply type {reply_type}")


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        broker = Broker(channel)
        queue = await channel.declare_queue(constants.ORDER_REPLIES_QUEUE, durable=True)

        await broker.publish_reserve_stock(order_id, "WIDGET-1", 2)

        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle_reply(broker, envelope)


if __name__ == "__main__":
    asyncio.run(main())
