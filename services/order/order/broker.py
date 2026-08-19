import aio_pika
from eventing import Envelope, constants


class Broker:
    def __init__(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self.channel = channel

    async def publish_reserve_stock(self, order_id: str, sku: str, qty: int) -> None:
        envelope = Envelope(
            type="ReserveStock",
            payload={"order_id": order_id, "sku": sku, "qty": qty},
        )
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key=constants.RESERVE_STOCK_QUEUE,
        )

    async def publish_charge_payment(self, order_id: str, sku: str, qty: int) -> None:
        envelope = Envelope(
            type="ChargePayment",
            payload={"order_id": order_id, "sku": sku, "qty": qty},
        )
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key=constants.CHARGE_PAYMENT_QUEUE,
        )

    async def publish_release_stock(self, order_id: str, sku: str, qty: int) -> None:
        envelope = Envelope(
            type="ReleaseStock",
            payload={"order_id": order_id, "sku": sku, "qty": qty},
        )
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key=constants.RESERVE_STOCK_QUEUE,
        )

    async def publish_notification(
        self, outcome_type: str, order_id: str, sku: str, qty: int
    ) -> None:
        envelope = Envelope(
            type=outcome_type,  # "OrderConfirmed" or "OrderFailed"
            payload={"order_id": order_id, "sku": sku, "qty": qty},
        )
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=envelope.to_bytes(), message_id=envelope.message_id),
            routing_key=constants.NOTIFICATIONS_QUEUE,
        )
