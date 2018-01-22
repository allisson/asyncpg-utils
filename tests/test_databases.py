import pytest

from asyncpg_utils.databases import PoolDatabase

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_insert(selected_database, post_data):
    if isinstance(selected_database, PoolDatabase):
        await selected_database.init_pool()

    row = await selected_database.insert('posts', post_data)

    assert row['id']
    assert row['title'] == post_data['title']
    assert row['body'] == post_data['body']
    assert row['pub_date'] == post_data['pub_date']


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_query(selected_database, post_data):
    if isinstance(selected_database, PoolDatabase):
        await selected_database.init_pool()

    await selected_database.insert('posts', post_data)
    rows = await selected_database.query(
        """
        SELECT * FROM posts
        """
    )

    assert len(rows) == 1
    row = rows[0]
    assert row['id']
    assert row['title'] == post_data['title']
    assert row['body'] == post_data['body']
    assert row['pub_date'] == post_data['pub_date']


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_query_one(selected_database, post_data):
    if isinstance(selected_database, PoolDatabase):
        await selected_database.init_pool()

    await selected_database.insert('posts', post_data)
    row = await selected_database.query_one(
        """
        SELECT * FROM posts
        WHERE title = $1
        """,
        'Post Title'
    )

    assert row['id']
    assert row['title'] == post_data['title']
    assert row['body'] == post_data['body']
    assert row['pub_date'] == post_data['pub_date']


async def transaction_coroutine(conn, selected_database, post_data):
    async with conn.transaction():
        await selected_database.insert('posts', post_data, connection=conn, close_connection=False)
        await selected_database.insert('posts', post_data, connection=conn, close_connection=False)
        raise Exception('BOOM!')


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_transaction(selected_database, post_data):
    if isinstance(selected_database, PoolDatabase):
        await selected_database.init_pool()

    conn = await selected_database.get_connection()

    with pytest.raises(Exception):
        await transaction_coroutine(conn, selected_database, post_data)

    await conn.close()

    rows = await selected_database.query(
        """
        SELECT * FROM posts
        """
    )
    assert len(rows) == 0
