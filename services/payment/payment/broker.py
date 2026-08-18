import aio_pika
from eventing import Envelope

REPLY_QUEUE = "order_replies"


class Broker:
    def __init__(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self.channel = channel

    async def publish_reply(self, reply_type: str, order_id: str) -> None:
        envelope = Envelope(type=reply_type, payload={"order_id": order_id})
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key=REPLY_QUEUE,
        )
