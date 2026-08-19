"""Force the race condition into the open.

Fires many reserve operations at the same time. Each does the naive
read -> check -> write. Because they overlap in the read-then-write gap,
they oversell: more get 'reserved' than actually existed.
"""

import asyncio

import asyncpg

from inventory.config import DB_URL


async def naive_reserve(pool: asyncpg.Pool, sku: str, qty: int, worker: int) -> None:
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE stock SET quantity = quantity - $1 WHERE sku = $2 AND quantity >= $1",
            qty,
            sku,
        )
        if result == "UPDATE 1":
            print(f"worker {worker}: reserved {qty}")
        else:
            print(f"worker {worker}: refused (not enough stock)")


async def main() -> None:
    pool = await asyncpg.create_pool(dsn=DB_URL)
    # 10 workers all try to reserve 1, at the same time, against stock of 5.
    # Correct behaviour: exactly 5 succeed, 5 refused, final stock 0.
    # Racy behaviour: MORE than 5 succeed, final stock goes negative or wrong.
    await asyncio.gather(*(naive_reserve(pool, "WIDGET-1", 1, i) for i in range(10)))
    final = await pool.fetchval("SELECT quantity FROM stock WHERE sku = $1", "WIDGET-1")
    print(f"\nFINAL STOCK: {final}  (should be 0 if no oversell)")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
