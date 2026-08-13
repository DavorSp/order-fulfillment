from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass(slots=True)
class Envelope:
    """What every message looks like on the wire.

    message_id is the idempotency key — a consumer uses it to answer
    'have I already processed this?' before acting.
    """

    type: str
    payload: dict
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        return cls(**json.loads(raw.decode()))
