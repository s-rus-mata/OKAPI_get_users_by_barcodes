import asyncio
from connection import Connection

# Ejemplo de uso
if __name__ == "__main__":
    async def main():
        conn = Connection()
        token = await conn.get_token()
        print(f"✅ Token: {token}")

    asyncio.run(main())