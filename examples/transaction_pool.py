import asyncio
from contextlib import suppress
from datetime import date

from asyncpg_utils.databases import PoolDatabase

loop = asyncio.get_event_loop()
database = PoolDatabase('postgresql://postgres:postgres@localhost/asyncpg-utils')


async def create_table():
    conn = await database.get_connection()
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users(
            id serial PRIMARY KEY,
            name text,
            dob date
        )
        """
    )
    await conn.close()
    return True


async def transaction_coroutine(conn):
    async with conn.transaction():
        await database.insert('users', {'name': 'John Doe', 'dob': date(2000, 1, 1)}, connection=conn, close_connection=False)
        raise Exception('BOOM!')


async def main():
    await database.init_pool()
    print('create_table users, {!r}'.format(await create_table()))
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

loop.run_until_complete(main())
