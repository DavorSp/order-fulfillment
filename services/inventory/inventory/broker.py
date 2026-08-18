import aio_pika
from eventing import Envelope


class Broker:
    # Owns the channel — the thing we publish through. Same idea as
    # StockRepository owning the pool: state bundled with the behavior that uses it.
    def __init__(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self.channel = channel

    # Publish a reply message onto the given queue.
    async def publish_reply(self, reply_type: str, order_id: str, sku: str, qty: int) -> None:
        # 1. build an Envelope: type=reply_type, payload carries order_id/sku/qty
        envelope = Envelope(type=reply_type, payload={"order_id": order_id, "sku": sku, "qty": qty})
        # 2. publish it to the "stock_replies" queue via self.channel
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key="order_replies",
        )
