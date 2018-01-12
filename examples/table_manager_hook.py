import asyncio
from datetime import date

from asyncpg_utils.databases import Database
from asyncpg_utils.managers import AbstractHook, TableManager

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
user_data = {
    'name': 'Allisson',
    'dob': date(1983, 2, 9)
}


class TestHook(AbstractHook):
    async def pre_create(self, data):
        print('pre_create, data={!r}'.format(data))

    async def post_create(self, row):
        print('post_create, row={!r}'.format(row))

    async def pre_list(self, fields, filters, order_by, order_by_sort):
        print('pre_list, fields={!r}, filters={!r}, order_by={!r}, order_by_sort={!r}'.format(fields, filters, order_by, order_by_sort))

    async def post_list(self, rows):
        print('post_list, rows={!r}'.format(rows))

    async def pre_detail(self, pk, pk_field, fields):
        print('pre_detail, pk={!r}, pk_field={!r}, fields={!r}'.format(pk, pk_field, fields))

    async def post_detail(self, row):
        print('post_detail, row={!r}'.format(row))

    async def pre_update(self, pk, data):
        print('pre_update, pk={!r}, data={!r}'.format(pk, data))

    async def post_update(self, row):
        print('post_update, row={!r}'.format(row))

    async def pre_delete(self, pk):
        print('pre_delete, pk={!r}'.format(pk))

    async def post_delete(self, pk):
        print('post_delete, pk={!r}'.format(pk))


table_manager = TableManager(database, 'users', pk_field='id', hooks=(TestHook,))


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


async def main():
    print('create_table users, {!r}'.format(await create_table()))
    await table_manager.create(user_data)
    await table_manager.list()
    await table_manager.detail(1)
    user_data['name'] = 'John Doe'
    await table_manager.update(1, user_data)
    await table_manager.delete(1)


loop.run_until_complete(main())
