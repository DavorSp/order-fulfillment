"""Queue names — the single source of truth for publisher/consumer pairing.

A mismatch between a publisher's queue name and a consumer's is a silent
failure: no error, no message, nothing happens. Importing both sides from
here makes that class of bug unrepresentable.
"""

from typing import Final

RESERVE_STOCK_QUEUE: Final = "reserve_stock"
CHARGE_PAYMENT_QUEUE: Final = "charge_payment"
ORDER_REPLIES_QUEUE: Final = "order_replies"
NOTIFICATIONS_QUEUE: Final = "notifications"
CREATE_ORDER_QUEUE: Final = "create_order"
