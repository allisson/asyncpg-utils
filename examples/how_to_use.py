import asyncio
from datetime import date

from asyncpg_utils.databases import Database

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')


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
    print('create_table users, {!r}'.format(await create_table()))
    print('insert row, {!r}'.format(await insert_row({'name': 'John Doe', 'dob': date(2000, 1, 1)})))
    print('insert row, {!r}'.format(await insert_row({'name': 'Jane Doe', 'dob': date(2000, 1, 1)})))
    print('query all results, {!r}'.format(await query_all()))
    print('query one result, {!r}'.format(await query_one()))

loop.run_until_complete(main())
