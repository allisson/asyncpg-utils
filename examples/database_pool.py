import asyncio
from datetime import date

from asyncpg_utils.databases import PoolDatabase

from utils import create_table, drop_table

loop = asyncio.get_event_loop()
database = PoolDatabase('postgresql://postgres:postgres@localhost/asyncpg-utils')


async def insert_row(data):
    return await database.insert('users', data)


async def query_all():
    return await database.query(
        """
        SELECT * FROM users
        """
    )


async def query_one():
    return await database.query_one(
        """
        SELECT * FROM users
        WHERE name = $1
        """,
        'Jane Doe'
    )


async def main():
    await database.init_pool()
    await create_table(database)
    print('insert row, {!r}'.format(await insert_row({'name': 'John Doe', 'dob': date(2000, 1, 1)})))
    print('insert row, {!r}'.format(await insert_row({'name': 'Jane Doe', 'dob': date(2000, 1, 1)})))
    print('query all results, {!r}'.format(await query_all()))
    print('query one result, {!r}'.format(await query_one()))
    await drop_table(database)

loop.run_until_complete(main())
