import asyncio
from datetime import date

from asyncpg_utils.databases import Database
from asyncpg_utils.managers import AbstractHook, TableManager

from utils import create_table, drop_table

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
user_data = {
    'name': 'Allisson',
    'dob': date(1983, 2, 9)
}


class TestHook(AbstractHook):
    async def pre_create(self, *args, **kwargs):
        print('pre_create, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def post_create(self, *args, **kwargs):
        print('post_create, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def pre_list(self, *args, **kwargs):
        print('pre_list, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def post_list(self, *args, **kwargs):
        print('post_list, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def pre_detail(self, *args, **kwargs):
        print('pre_detail, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def post_detail(self, *args, **kwargs):
        print('post_detail, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def pre_update(self, *args, **kwargs):
        print('pre_update, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def post_update(self, *args, **kwargs):
        print('post_update, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def pre_delete(self, *args, **kwargs):
        print('pre_delete, args={!r}, kwargs={!r}'.format(args, kwargs))

    async def post_delete(self, *args, **kwargs):
        print('post_delete, args={!r}, kwargs={!r}'.format(args, kwargs))


table_manager = TableManager(database, 'users', pk_field='id', hooks=(TestHook,))


async def main():
    await create_table(database)
    await table_manager.create(user_data)
    await table_manager.list()
    await table_manager.detail(1)
    user_data['name'] = 'John Doe'
    await table_manager.update(1, user_data)
    await table_manager.delete(1)
    await drop_table(database)


loop.run_until_complete(main())
