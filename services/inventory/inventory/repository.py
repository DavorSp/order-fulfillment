import asyncpg


class StockRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def reserve(self, sku: str, qty: int) -> bool:
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                "UPDATE stock SET quantity = quantity - $1 WHERE sku = $2 AND quantity >= $1",
                qty,
                sku,
            )
            return bool(result == "UPDATE 1")

    async def release(self, sku: str, qty: int) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE stock SET quantity = quantity + $1 WHERE sku = $2",
                qty,
                sku,
            )
