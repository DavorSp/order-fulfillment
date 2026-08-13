import asyncio
import asyncpg

async def main():
    db_url = "postgresql://inventory:inventory@localhost:5432/inventory"
    connection = await asyncpg.connect(db_url)
    quantity = await connection.fetchval("SELECT quantity FROM stock WHERE sku = 'WIDGET-1'")
    print(f"Quantity for WIDGET-1: {quantity}")
    await connection.close()

if __name__ == "__main__":
    asyncio.run(main())