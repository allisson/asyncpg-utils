import asyncio
from datetime import date

from asyncpg_utils.databases import Database
from asyncpg_utils.managers import TableManager

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
table_manager = TableManager(database, 'users', pk_field='id', hooks=None)
user_data = {
    'name': 'Allisson',
    'dob': date(1983, 2, 9)
}


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


async def table_manager_create():
    print('table_manager.create, row={!r}'.format(await table_manager.create(user_data)))


async def table_manager_list():
    print('table_manager.list, rows={!r}'.format(await table_manager.list()))
    print('table_manager.list, only_name_field, rows={!r}'.format(await table_manager.list(fields=['name'])))
    print('table_manager.list, filter_by_id, rows={!r}'.format(await table_manager.list(filters={'id': 999999})))
    print('table_manager.list, order_by=name, order_by_sort=ASC, rows={!r}'.format(await table_manager.list(order_by='name', order_by_sort='ASC')))
    print('table_manager.list, count=True, rows={!r}'.format(await table_manager.list(count=True)))
    print('table_manager.list, limit=1, offset=0, rows={!r}'.format(await table_manager.list(limit=1, offset=0)))


async def table_manager_detail():
    print('table_manager.detail, row={!r}'.format(await table_manager.detail(1)))
    print('table_manager.detail, only_name_field, row={!r}'.format(await table_manager.detail(1, fields=['name'])))


async def table_manager_update():
    user_data['name'] = 'John Doe'
    print('table_manager.update, row={!r}'.format(await table_manager.update(1, user_data)))


async def table_manager_delete():
    print('table_manager.delete, result={!r}'.format(await table_manager.delete(1)))


async def main():
    print('create_table users, {!r}'.format(await create_table()))
    await table_manager_create()
    await table_manager_list()
    await table_manager_detail()
    await table_manager_update()
    await table_manager_delete()

loop.run_until_complete(main())
