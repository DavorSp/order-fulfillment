import asyncio

import asyncpg

from inventory.config import DB_URL


async def main() -> None:
    connection = await asyncpg.connect(DB_URL)
    quantity = await connection.fetchval("SELECT quantity FROM stock WHERE sku = 'WIDGET-1'")
    print(f"Quantity for WIDGET-1: {quantity}")
    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
