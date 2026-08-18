import asyncpg


class PaymentRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def charge(self, order_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO payments (order_id, status) VALUES ($1, $2)",
                order_id,
                "charged",
            )
