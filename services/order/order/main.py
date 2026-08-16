import asyncio

import aio_pika
from eventing import Envelope
from order.broker import Broker

AMQP_URL = "amqp://guest:guest@localhost/"
REPLY_QUEUE = "stock_replies"


async def handle_reply(envelope: Envelope) -> None:
    reply_type = envelope.type
    order_id = envelope.payload.get("order_id")
    if reply_type == "StockReserved":
        print(f"Order {order_id}: stock reserved — would proceed to payment")
    else:
        print(f"Order {order_id}: stock failed — order cannot proceed")


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        broker = Broker(channel)
        queue = await channel.declare_queue(REPLY_QUEUE, durable=True)

        await broker.publish_reserve_stock("order-123", "WIDGET-1", 2)

        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process():
                    envelope = Envelope.from_bytes(message.body)
                    await handle_reply(envelope)

if __name__ == "__main__":
    asyncio.run(main())