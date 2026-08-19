import asyncpg


class OrderRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def create_order(self, order_id: str, sku: str, qty: int) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                "INSERT INTO orders (order_id, sku, qty, status) "
                "VALUES ($1, $2, $3, 'pending') ON CONFLICT DO NOTHING",
                order_id,
                sku,
                qty,
            )

    async def set_order_status(self, order_id: str, status: str) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE orders SET status = $1, updated_at = now() WHERE order_id = $2",
                status,
                order_id,
            )

    async def get_order_status(self, order_id: str) -> str:
        async with self.pool.acquire() as connection:
            result = await connection.fetchrow(
                "SELECT status FROM orders WHERE order_id = $1", order_id
            )
            if result is None:
                raise LookupError(f"No order found with id {order_id}")
            status: str = result["status"]
            return status
