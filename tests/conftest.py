import asyncio
import os
import uuid
from datetime import date

import asyncpg
import asynctest
import pytest

from asyncpg_utils.databases import Database, PoolDatabase
from asyncpg_utils.managers import AbstractHook, TableManager

dsn = os.environ.get('DATABASE_URL')
loop = asyncio.get_event_loop()


async def create_table(dsn):
    conn = await asyncpg.connect(dsn)
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


async def drop_table(dsn):
    conn = await asyncpg.connect(dsn)
    await conn.execute(
        """
        DROP TABLE users
        """
    )
    await conn.close()


async def clear_table(dsn):
    conn = await asyncpg.connect(dsn)
    await conn.execute(
        """
        TRUNCATE TABLE users
        """
    )
    await conn.close()


@pytest.fixture(scope='session', autouse=True)
def table_setup(request):
    def teardown():
        loop.run_until_complete(drop_table(dsn))

    loop.run_until_complete(create_table(dsn))
    request.addfinalizer(teardown)


@pytest.fixture(scope='function', autouse=True)
def table_clear(request):
    def teardown():
        loop.run_until_complete(clear_table(dsn))

    request.addfinalizer(teardown)


@pytest.fixture
def database():
    return Database(dsn)


@pytest.fixture
def pool_database():
    return PoolDatabase(dsn)


@pytest.fixture
def user_data():
    return {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }


class TestHook(AbstractHook):
    def __init__(self, table_manager):
        super().__init__(table_manager)
        self.table_manager.hook_event_mock = asynctest.CoroutineMock()

    async def pre_create(self, *args, **kwargs):
        self.table_manager.hook_event_mock.pre_create(*args, **kwargs)

    async def post_create(self, *args, **kwargs):
        self.table_manager.hook_event_mock.post_create(*args, **kwargs)

    async def pre_list(self, *args, **kwargs):
        self.table_manager.hook_event_mock.pre_list(*args, **kwargs)

    async def post_list(self, *args, **kwargs):
        self.table_manager.hook_event_mock.post_list(*args, **kwargs)

    async def pre_detail(self, *args, **kwargs):
        self.table_manager.hook_event_mock.pre_detail(*args, **kwargs)

    async def post_detail(self, *args, **kwargs):
        self.table_manager.hook_event_mock.post_detail(*args, **kwargs)

    async def pre_update(self, *args, **kwargs):
        self.table_manager.hook_event_mock.pre_update(*args, **kwargs)

    async def post_update(self, *args, **kwargs):
        self.table_manager.hook_event_mock.post_update(*args, **kwargs)

    async def pre_delete(self, *args, **kwargs):
        self.table_manager.hook_event_mock.pre_delete(*args, **kwargs)

    async def post_delete(self, *args, **kwargs):
        self.table_manager.hook_event_mock.post_delete(*args, **kwargs)


@pytest.fixture
def table_manager(database):
    return TableManager(database, 'users', hooks=(TestHook,))
