import asyncio
from collections.abc import Awaitable, Callable

import aio_pika
import asyncpg
from eventing import Envelope, Idempotency, constants
from redis.asyncio import Redis

from order.broker import Broker
from order.config import AMQP_URL, DB_URL, REDIS_URL
from order.repository import OrderRepository


async def handle_create_order(repo: OrderRepository, broker: Broker, envelope: Envelope) -> None:
    order_id = envelope.payload["order_id"]
    sku = envelope.payload["sku"]
    qty = envelope.payload["qty"]

    # 1. persist the order as 'pending'
    await repo.create_order(order_id, sku, qty)
    # 2. print something useful
    print(f"Order {order_id}: created with status 'pending'")
    # 3. publish ReserveStock
    await broker.publish_reserve_stock(order_id, sku, qty)


async def handle_reply(repo: OrderRepository, broker: Broker, envelope: Envelope) -> None:
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


async def consume(
    channel: aio_pika.abc.AbstractChannel,
    queue_name: str,
    handler: Callable[[OrderRepository, Broker, Envelope], Awaitable[None]],
    repo: OrderRepository,
    broker: Broker,
    idempotency: Idempotency,
) -> None:
    queue = await channel.declare_queue(queue_name, durable=True)
    async with queue.iterator() as messages:
        async for message in messages:
            async with message.process():
                envelope = Envelope.from_bytes(message.body)
                if not await idempotency.is_new(envelope.message_id):
                    print(f"Skipping duplicate {envelope.message_id}")
                    continue
                await handler(repo, broker, envelope)


async def main() -> None:
    redis = Redis.from_url(REDIS_URL)
    idempotency = Idempotency(redis)
    pool = await asyncpg.create_pool(dsn=DB_URL)
    repo = OrderRepository(pool)

    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        create_channel = await connection.channel()
        reply_channel = await connection.channel()
        broker = Broker(create_channel)

        await asyncio.gather(
            consume(
                create_channel,
                constants.CREATE_ORDER_QUEUE,
                handle_create_order,
                repo,
                broker,
                idempotency,
            ),
            consume(
                reply_channel,
                constants.ORDER_REPLIES_QUEUE,
                handle_reply,
                repo,
                broker,
                idempotency,
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
