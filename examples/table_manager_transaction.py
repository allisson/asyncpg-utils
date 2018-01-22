import asyncio
from contextlib import suppress
from datetime import date

from asyncpg_utils.databases import Database
from asyncpg_utils.managers import TableManager

from utils import create_table, drop_table

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
table_manager = TableManager(database, 'users', pk_field='id', hooks=None)
user_data = {
    'name': 'Allisson',
    'dob': date(1983, 2, 9)
}


async def main():
    await create_table(database)

    conn = await table_manager.database.get_connection()
    with suppress(Exception):
        async with conn.transaction():
            await table_manager.create(user_data, connection=conn, close_connection=False)
            raise Exception('BOOM')
    await conn.close()
    result = await table_manager.list(count=True)
    print('table users count, {}'.format(result[0]['count']))

    await drop_table(database)

loop.run_until_complete(main())
