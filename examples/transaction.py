import asyncio
from contextlib import suppress
from datetime import date

from asyncpg_utils.databases import Database

from utils import create_table, drop_table

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')


async def transaction_coroutine(conn):
    async with conn.transaction():
        await database.insert('users', {'name': 'John Doe', 'dob': date(2000, 1, 1)}, connection=conn, close_connection=False)
        raise Exception('BOOM!')


async def main():
    await create_table(database)
    conn = await database.get_connection()

    with suppress(Exception):
        await transaction_coroutine(conn)

    await conn.close()

    result = await database.query_one(
        """
        SELECT COUNT(1) FROM users
        """
    )
    print('table users count, {}'.format(result['count']))
    await drop_table(database)

loop.run_until_complete(main())
