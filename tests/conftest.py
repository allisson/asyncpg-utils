import asyncio
import os
from datetime import date

import asyncpg
import pytest

from asyncpg_utils.databases import Database, PoolDatabase

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
