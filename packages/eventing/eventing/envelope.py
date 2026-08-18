from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TypedDict


class OrderPayload(TypedDict):
    order_id: str
    sku: str
    qty: int


@dataclass(slots=True)
class Envelope:
    type: str
    payload: OrderPayload  # <-- was dict[str, str | int]
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        return cls(**json.loads(raw.decode()))
