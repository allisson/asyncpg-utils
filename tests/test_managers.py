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


async def test_table_manager_list_with_count(table_manager, user_data):
    await table_manager.create(user_data)
    rows = await table_manager.list(count=True)
    assert len(rows) == 1
    row = rows[0]
    assert row['count'] == 1


async def test_table_manager_list_with_exact_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)

    rows = await table_manager.list(filters={'id': user1_row['id']})
    assert len(rows) == 1
    assert user1_row in rows
    assert user2_row not in rows

    rows = await table_manager.list(filters={'id__exact': user1_row['id']})
    assert len(rows) == 1
    assert user1_row in rows
    assert user2_row not in rows


async def test_table_manager_list_with_like_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'name__like': '%Doe%'})
    assert len(rows) == 1
    assert user1_row not in rows
    assert user2_row in rows


async def test_table_manager_list_with_ilike_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'name__ilike': '%doe%'})
    assert len(rows) == 1
    assert user1_row not in rows
    assert user2_row in rows


async def test_table_manager_list_with_in_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'id__in': [user1_row['id'], user2_row['id']]})
    assert len(rows) == 2
    assert user1_row in rows
    assert user2_row in rows


async def test_table_manager_list_with_gt_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'dob__gt': user1_data['dob']})
    assert len(rows) == 1
    assert user1_row not in rows
    assert user2_row in rows


async def test_table_manager_list_with_gte_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'dob__gte': user1_data['dob']})
    assert len(rows) == 2
    assert user1_row in rows
    assert user2_row in rows


async def test_table_manager_list_with_lt_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'dob__lt': user2_data['dob']})
    assert len(rows) == 1
    assert user1_row in rows
    assert user2_row not in rows


async def test_table_manager_list_with_lte_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    rows = await table_manager.list(filters={'dob__lte': user2_data['dob']})
    assert len(rows) == 2
    assert user1_row in rows
    assert user2_row in rows


async def test_table_manager_list_with_multiple_filters(table_manager):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)

    rows = await table_manager.list(
        filters={'name': user1_data['name'], 'dob': user1_row['dob']},
        filters_operator='AND'
    )
    assert len(rows) == 1
    assert user1_row in rows
    assert user2_row not in rows

    rows = await table_manager.list(
        filters={'name': user1_data['name'], 'dob': user2_row['dob']},
        filters_operator='OR'
    )
    assert len(rows) == 2
    assert user1_row in rows
    assert user2_row in rows


async def test_table_manager_list_with_order_by(table_manager, user_data):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    await table_manager.create(user1_data)
    await table_manager.create(user2_data)

    rows = await table_manager.list(order_by='name', order_by_sort='ASC')
    assert rows[0]['name'] == 'Allisson'
    assert rows[1]['name'] == 'John Doe'

    rows = await table_manager.list(order_by='name', order_by_sort='DESC')
    assert rows[0]['name'] == 'John Doe'
    assert rows[1]['name'] == 'Allisson'


async def test_table_manager_list_with_limit_offset(table_manager, user_data):
    user1_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }
    user2_data = {
        'name': 'John Doe',
        'dob': date(2000, 1, 1)
    }
    await table_manager.create(user1_data)
    await table_manager.create(user2_data)

    rows = await table_manager.list(limit=1, offset=0)
    assert len(rows) == 1
    assert rows[0]['name'] == 'Allisson'

    rows = await table_manager.list(limit=1, offset=1)
    assert len(rows) == 1
    assert rows[0]['name'] == 'John Doe'


async def test_table_manager_list_with_joins(post_table, comment_table, post_data, comment_data):
    fields = [
        'comments.id',
        'comments.body',
        'comments.pub_date',
        'posts.title as post_title',
        'posts.body as post_body',
        'posts.pub_date as post_pub_date'
    ]
    joins = {'posts': {'type': 'INNER JOIN', 'source': 'comments.post_id', 'target': 'posts.id'}}
    post_row = await post_table.create(post_data)
    comment_data['post_id'] = post_row['id']
    comment_row = await comment_table.create(comment_data)

    rows = await comment_table.list(fields=fields, joins=joins, limit=1, offset=0)
    assert len(rows) == 1
    row = rows[0]
    assert row['id'] == comment_row['id']
    assert row['body'] == comment_row['body']
    assert row['pub_date'] == comment_row['pub_date']
    assert row['post_title'] == post_row['title']
    assert row['post_body'] == post_row['body']
    assert row['post_pub_date'] == post_row['pub_date']


async def test_table_manager_detail(table_manager, user_data):
    created_row = await table_manager.create(user_data)
    selected_row = await table_manager.detail(created_row['id'])
    assert created_row['id'] == selected_row['id']
    assert selected_row['id']
    assert selected_row['name'] == user_data['name']
    assert selected_row['dob'] == user_data['dob']
    assert table_manager.hook_event_mock.pre_detail.called is True
    assert table_manager.hook_event_mock.post_detail.called is True


async def test_table_manager_detail_with_fields(table_manager, user_data):
    created_row = await table_manager.create(user_data)
    selected_row = await table_manager.detail(created_row['id'], fields=['name'])
    assert created_row['name'] == selected_row['name']
    assert 'id' not in selected_row
    assert selected_row['name'] == user_data['name']
    assert 'dob' not in selected_row


async def test_table_manager_detail_with_other_pk_field(table_manager, user_data):
    created_row = await table_manager.create(user_data)
    selected_row = await table_manager.detail(created_row['name'], pk_field='name')
    assert created_row['id'] == selected_row['id']
    assert selected_row['id']
    assert selected_row['name'] == user_data['name']
    assert selected_row['dob'] == user_data['dob']


async def test_table_manager_update(table_manager, user_data):
    row = await table_manager.create(user_data)
    id = row['id']
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
    id = row['id']
    assert await table_manager.delete(id) is True
    rows = await table_manager.list()
    assert len(rows) == 0
    assert table_manager.hook_event_mock.pre_delete.called is True
    assert table_manager.hook_event_mock.post_delete.called is True


async def test_hook_trigger_invalid_event(table_manager, user_data):
    hook = table_manager.hooks[0]
    assert await hook.trigger_event('invalid_event') is None


@pytest.mark.parametrize('filters,expected_result', [
    ({'field': 'value'}, {'field': {'lookup': 'exact', 'value': 'value'}}),
    ({'field__exact': 'value'}, {'field': {'lookup': 'exact', 'value': 'value'}}),
    ({'field__like': 'value'}, {'field': {'lookup': 'like', 'value': 'value'}}),
    ({'field__ilike': 'value'}, {'field': {'lookup': 'ilike', 'value': 'value'}}),
    ({'field__in': 'value'}, {'field': {'lookup': 'in', 'value': 'value'}}),
    ({'field__gt': 'value'}, {'field': {'lookup': 'gt', 'value': 'value'}}),
    ({'field__gte': 'value'}, {'field': {'lookup': 'gte', 'value': 'value'}}),
    ({'field__lt': 'value'}, {'field': {'lookup': 'lt', 'value': 'value'}}),
    ({'field__lte': 'value'}, {'field': {'lookup': 'lte', 'value': 'value'}}),
])
async def test_table_manager_parse_filters(filters, expected_result, table_manager):
    result = table_manager.parse_filters(filters)
    assert result == expected_result
