import asyncio
import sys

import aio_pika
from eventing import Envelope, constants
from order.config import AMQP_URL

order_id = sys.argv[1] if len(sys.argv) > 1 else "order-123"


async def main() -> None:
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()
        envelope = Envelope(
            type="CreateOrder",
            payload={"order_id": order_id, "sku": "WIDGET-1", "qty": 2},
        )
        await channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes()),
            routing_key=constants.CREATE_ORDER_QUEUE,
        )
        print(f"Published CreateOrder for {order_id}")


if __name__ == "__main__":
    asyncio.run(main())
