import asyncio

from streamer.Db import DbManager


async def init_db():
    db_manager = DbManager()
    await db_manager.create_db()


if __name__ == '__main__':
    asyncio.run(init_db())

