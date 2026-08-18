# Order Fulfillment

An event-driven order fulfillment system built as a learning project in
distributed-systems mechanics. The goal is to get the *mechanics* right —
saga orchestration, per-service data ownership, idempotent message
processing, compensating transactions — not to ship a product.

## Architecture

Four services are planned; three are implemented so far.

| Service | Role | Owns | Status |
|---|---|---|---|
| **order** | Saga orchestrator — kicks off the order, reacts to replies, triggers compensation | *(no DB yet — in-memory per run)* | Implemented |
| **inventory** | Reserves / releases stock | Postgres (`stock` table) | Implemented |
| **payment** | Charges / fails payment | Postgres (`payments` table) | Implemented |
| **notification** | Notifies the customer of the outcome | — | Not yet built |

Rules the architecture follows:
- **No shared tables.** Each service has its own Postgres database. Services
  never touch another service's DB directly.
- **No direct service-to-service calls.** All cross-service communication
  goes through RabbitMQ as commands and reply events.
- **Idempotent consumers.** Redis holds processed `message_id`s (1 hour TTL)
  so redelivered messages are skipped instead of reprocessed.

```
                 ┌─────────┐
   ReserveStock  │         │  ChargePayment
   ┌─────────────┤  order  ├─────────────┐
   │             │         │             │
   │             └────▲────┘             │
   │                  │ replies          │
   ▼                  │ (order_replies)  ▼
┌───────────┐         │           ┌───────────┐
│ inventory │─────────┴───────────│  payment  │
│ (Postgres)│                     │ (Postgres)│
└───────────┘                     └───────────┘
     ▲
     │ ReleaseStock (compensation, sent by order
     └── if payment fails after stock was reserved)
```

### Message flow (the saga)

**Happy path:**
1. `order` publishes `ReserveStock(order_id, sku, qty)` → `reserve_stock` queue.
2. `inventory` atomically checks and decrements stock, replies `StockReserved`
   → `order_replies` queue.
3. `order` sees `StockReserved`, publishes `ChargePayment(order_id, sku, qty)`
   → `charge_payment` queue.
4. `payment` charges (inserts a `payments` row), replies `PaymentCharged`
   → `order_replies`.
5. `order` sees `PaymentCharged` → order confirmed.

**Failure + compensation:**
- If stock isn't available, `inventory` replies `StockFailed` instead —
  order stops there, nothing to compensate.
- If payment fails *after* stock was already reserved, `payment` replies
  `PaymentFailed`. `order` reacts by publishing `ReleaseStock(order_id, sku,
  qty)` back to `inventory`, which increments the stock back — the
  compensating transaction that keeps inventory correct despite the failed
  saga.
- (`payment`'s test harness treats `order_id == "order-fail"` as a magic
  value that always fails, for exercising this path on demand.)

All queues are durable; every consumer checks Redis via
`Idempotency.is_new(message_id)` before acting, so redelivery from RabbitMQ
never double-reserves stock or double-charges a payment.

## Repo layout

```
packages/eventing/          # shared library, no service-specific logic
  eventing/envelope.py         Envelope — the on-wire message shape (type, payload, message_id)
  eventing/idempotency.py      Idempotency — Redis-backed dedup by message_id

services/order/
  order/main.py                entry point: publishes ReserveStock, reacts to replies
  order/broker.py               publishes ReserveStock / ChargePayment / ReleaseStock

services/inventory/
  inventory/consumer.py         entry point + StockRepository (reserve/release, atomic UPDATE)
  inventory/broker.py           publishes StockReserved/StockFailed replies
  inventory/db.py               ad hoc query script (not wired into the service)
  inventory/race_test.py        manual demo: reproduces the oversell race condition

services/payment/
  payment/consumer.py           entry point + charge/fail logic
  payment/broker.py             publishes PaymentCharged/PaymentFailed replies
  payment/repository.py         PaymentRepository (inserts charge record)
  payment/test_publish.py       manual script: send one ChargePayment
  payment/test_duplicate.py     manual script: send the same message twice (idempotency demo)

infra/compose/
  docker-compose.yml            rabbitmq, inventory's postgres, payment's postgres, redis
  01_schema.sql / 02_seed.sql   inventory DB schema + dev seed data
  payment_schema.sql            payment DB schema
```

Each service is its own `uv` workspace member with its own `pyproject.toml`;
`eventing` is a shared, installable package the services depend on.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker + Docker Compose

## Running it

```bash
uv sync                                                    # install all workspace deps
docker compose -f infra/compose/docker-compose.yml up -d   # rabbitmq, 2x postgres, redis
```

Then run each service as its own process (separate terminals):

```bash
uv run python -m inventory.consumer
uv run python -m payment.consumer
uv run python -m order.main [order_id]     # defaults to order-123; try order-fail
```

Or launch all three together, labeled, with `./testrun.sh [order_id]` (resets
the databases on each run).

Useful endpoints once the stack is up:
- RabbitMQ management UI: http://localhost:15672 (guest/guest)
- inventory Postgres: `localhost:5432` (db/user/pass: `inventory`)
- payment Postgres: `localhost:5433` (db/user/pass: `payment`)
- Redis: `localhost:6379`

## Tooling

```bash
uv run pytest              # test suite
uv run ruff check .        # lint
uv run ruff format .       # format
uv run mypy .              # strict type checking
uv run pre-commit run --all-files   # everything above, as pre-commit runs it
```

`pre-commit install` wires these into `git commit` automatically (see
[.pre-commit-config.yaml](.pre-commit-config.yaml)).
