import uuid
from datetime import date

import pytest

pytestmark = pytest.mark.asyncio


async def test_table_manager_create(table_manager, user_data):
    row = await table_manager.create(user_data)
    assert row['id']
    assert row['name'] == user_data['name']
    assert row['dob'] == user_data['dob']
    assert table_manager.hook_event_mock.pre_create.called is True
    assert table_manager.hook_event_mock.post_create.called is True


async def test_table_manager_list(table_manager, user_data):
    await table_manager.create(user_data)
    rows = await table_manager.list()
    assert len(rows) == 1
    row = rows[0]
    assert row['id']
    assert row['name'] == user_data['name']
    assert row['dob'] == user_data['dob']
    assert table_manager.hook_event_mock.pre_list.called is True
    assert table_manager.hook_event_mock.post_list.called is True


async def test_table_manager_list_with_fields(table_manager, user_data):
    await table_manager.create(user_data)
    rows = await table_manager.list(fields=['name'])
    assert len(rows) == 1
    row = rows[0]
    assert 'id' not in row
    assert row['name'] == user_data['name']
    assert 'dob' not in row


async def test_table_manager_list_with_filters(table_manager, user_data):
    user1_data = {
        'id': str(uuid.uuid4()),
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'id': str(uuid.uuid4()),
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    await table_manager.create(user1_data)
    await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'id': str(user1_data['id'])})
    assert len(rows) == 1
    row = rows[0]
    assert str(row['id']) == user1_data['id']


async def test_table_manager_detail(table_manager, user_data):
    created_row = await table_manager.create(user_data)
    selected_row = await table_manager.detail(str(created_row['id']))
    assert created_row['id'] == selected_row['id']
    assert selected_row['id']
    assert selected_row['name'] == user_data['name']
    assert selected_row['dob'] == user_data['dob']
    assert table_manager.hook_event_mock.pre_detail.called is True
    assert table_manager.hook_event_mock.post_detail.called is True


async def test_table_manager_detail_with_fields(table_manager, user_data):
    created_row = await table_manager.create(user_data)
    selected_row = await table_manager.detail(str(created_row['id']), fields=['name'])
    assert created_row['name'] == selected_row['name']
    assert 'id' not in selected_row
    assert selected_row['name'] == user_data['name']
    assert 'dob' not in selected_row


async def test_table_manager_update(table_manager, user_data):
    row = await table_manager.create(user_data)
    id = str(row['id'])
    assert row['name'] == user_data['name']
    assert row['dob'] == user_data['dob']
    john_doe_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    row = await table_manager.update(id, john_doe_data)
    assert row['name'] == john_doe_data['name']
    assert row['dob'] == john_doe_data['dob']
    assert table_manager.hook_event_mock.pre_update.called is True
    assert table_manager.hook_event_mock.post_update.called is True


async def test_table_manager_delete(table_manager, user_data):
    row = await table_manager.create(user_data)
    id = str(row['id'])
    assert await table_manager.delete(id) is True
    rows = await table_manager.list()
    assert len(rows) == 0
    assert table_manager.hook_event_mock.pre_delete.called is True
    assert table_manager.hook_event_mock.post_delete.called is True


async def test_hook_trigger_invalid_event(table_manager, user_data):
    hook = table_manager.hooks[0]
    assert await hook.trigger_event('invalid_event') is None
