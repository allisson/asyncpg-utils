import pytest

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_insert(selected_database, user_data):
    row = await selected_database.insert('users', user_data)
    assert row['id']
    assert row['name'] == user_data['name']
    assert row['dob'] == user_data['dob']


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_query(selected_database, user_data):
    await selected_database.insert('users', user_data)
    rows = await selected_database.query(
        """
        SELECT * FROM users
        """
    )
    assert len(rows) == 1
    row = rows[0]
    assert row['id']
    assert row['name'] == user_data['name']
    assert row['dob'] == user_data['dob']


@pytest.mark.parametrize(
    'selected_database', (
        pytest.lazy_fixture('database'),
        pytest.lazy_fixture('pool_database')
    )
)
async def test_database_query_one(selected_database, user_data):
    await selected_database.insert('users', user_data)
    row = await selected_database.query_one(
        """
        SELECT * FROM users
        WHERE name = $1
        """,
        'Allisson'
    )
    assert row['id']
    assert row['name'] == user_data['name']
    assert row['dob'] == user_data['dob']
