import aio_pika
from eventing import Envelope

RESERVE_STOCK_QUEUE = "reserve_stock"


class Broker:
    def __init__(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self.channel = channel

    async def publish_reserve_stock(self, order_id: str, sku: str, qty: int) -> None:
        # build a ReserveStock envelope (payload carries order_id, sku, qty)
        envelope= Envelope(type="ReserveStock", payload={"order_id": order_id, "sku": sku, "qty": qty})
        # and publish it to the reserve_stock queue via self.channel
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(),message_id=envelope.message_id),
            routing_key=RESERVE_STOCK_QUEUE,
        )