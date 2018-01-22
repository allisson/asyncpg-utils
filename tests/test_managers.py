from contextlib import suppress
from datetime import datetime

import pytest

pytestmark = pytest.mark.asyncio


async def test_post_table_create(post_table, post_data):
    row = await post_table.create(post_data)
    assert row['id']
    assert row['title'] == post_data['title']
    assert row['body'] == post_data['body']
    assert row['pub_date'] == post_data['pub_date']
    assert post_table.hook_event_mock.pre_create.called is True
    assert post_table.hook_event_mock.post_create.called is True


async def test_post_table_list(post_table, post_data):
    await post_table.create(post_data)
    rows = await post_table.list()
    assert len(rows) == 1
    row = rows[0]
    assert row['id']
    assert row['title'] == post_data['title']
    assert row['body'] == post_data['body']
    assert row['pub_date'] == post_data['pub_date']
    assert post_table.hook_event_mock.pre_list.called is True
    assert post_table.hook_event_mock.post_list.called is True


async def test_post_table_list_with_fields(post_table, post_data):
    await post_table.create(post_data)
    rows = await post_table.list(fields=['title'])
    assert len(rows) == 1
    row = rows[0]
    assert 'id' not in row
    assert row['title'] == post_data['title']
    assert 'body' not in row
    assert 'pub_date' not in row


async def test_post_table_list_with_count(post_table, post_data):
    await post_table.create(post_data)
    rows = await post_table.list(count=True)
    assert len(rows) == 1
    row = rows[0]
    assert row['count'] == 1


async def test_post_table_list_with_exact_filters(post_table, post_data):
    post1_row = await post_table.create(post_data)
    post2_row = await post_table.create(post_data)

    rows = await post_table.list(filters={'id': post1_row['id']})
    assert len(rows) == 1
    assert post1_row in rows
    assert post2_row not in rows

    rows = await post_table.list(filters={'id__exact': post1_row['id']})
    assert len(rows) == 1
    assert post1_row in rows
    assert post2_row not in rows


async def test_post_table_list_with_like_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['title'] = 'Post about Allisson'
    post2_data['title'] = 'Post about John Doe'
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)
    rows = await post_table.list(filters={'title__like': '%Doe%'})
    assert len(rows) == 1
    assert post1_row not in rows
    assert post2_row in rows


async def test_post_table_list_with_ilike_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['title'] = 'Post about Allisson'
    post2_data['title'] = 'Post about John Doe'
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)
    rows = await post_table.list(filters={'title__ilike': '%doe%'})
    assert len(rows) == 1
    assert post1_row not in rows
    assert post2_row in rows


async def test_post_table_list_with_in_filters(post_table, post_data):
    post1_row = await post_table.create(post_data)
    post2_row = await post_table.create(post_data)
    rows = await post_table.list(filters={'id__in': [post1_row['id'], post2_row['id']]})
    assert len(rows) == 2
    assert post1_row in rows
    assert post2_row in rows
    rows = await post_table.list(filters={'id__in': [post1_row['id']]})
    assert len(rows) == 1
    assert post1_row in rows
    assert post2_row not in rows


async def test_post_table_list_with_gt_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['pub_date'] = datetime(2018, 1, 1, 0, 0, 0)
    post2_data['pub_date'] = datetime(2018, 1, 2, 0, 0, 0)
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)
    rows = await post_table.list(filters={'pub_date__gt': post1_data['pub_date']})
    assert len(rows) == 1
    assert post1_row not in rows
    assert post2_row in rows


async def test_post_table_list_with_gte_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['pub_date'] = datetime(2018, 1, 1, 0, 0, 0)
    post2_data['pub_date'] = datetime(2018, 1, 2, 0, 0, 0)
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)
    rows = await post_table.list(filters={'pub_date__gte': post1_data['pub_date']})
    assert len(rows) == 2
    assert post1_row in rows
    assert post2_row in rows


async def test_post_table_list_with_lt_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['pub_date'] = datetime(2018, 1, 1, 0, 0, 0)
    post2_data['pub_date'] = datetime(2018, 1, 2, 0, 0, 0)
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)
    rows = await post_table.list(filters={'pub_date__lt': post2_data['pub_date']})
    assert len(rows) == 1
    assert post1_row in rows
    assert post2_row not in rows


async def test_post_table_list_with_lte_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['pub_date'] = datetime(2018, 1, 1, 0, 0, 0)
    post2_data['pub_date'] = datetime(2018, 1, 2, 0, 0, 0)
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)
    rows = await post_table.list(filters={'pub_date__lte': post2_data['pub_date']})
    assert len(rows) == 2
    assert post1_row in rows
    assert post2_row in rows


async def test_post_table_list_with_multiple_filters(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['title'] = 'Title 1'
    post2_data['title'] = 'Title 2'
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)

    rows = await post_table.list(
        filters={'title': post1_data['title'], 'pub_date': post1_row['pub_date']},
        filters_operator='AND'
    )
    assert len(rows) == 1
    assert post1_row in rows
    assert post2_row not in rows

    rows = await post_table.list(
        filters={'title': post1_data['title'], 'pub_date': post2_row['pub_date']},
        filters_operator='OR'
    )
    assert len(rows) == 2
    assert post1_row in rows
    assert post2_row in rows


async def test_post_table_list_with_order_by(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['title'] = 'Title 1'
    post2_data['title'] = 'Title 2'
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)

    rows = await post_table.list(order_by='title', order_by_sort='ASC')
    assert rows[0]['title'] == post1_row['title']
    assert rows[1]['title'] == post2_row['title']

    rows = await post_table.list(order_by='title', order_by_sort='DESC')
    assert rows[0]['title'] == post2_row['title']
    assert rows[1]['title'] == post1_row['title']


async def test_post_table_list_with_limit_offset(post_table, post_data):
    post1_data = post_data.copy()
    post2_data = post_data.copy()
    post1_data['title'] = 'Title 1'
    post2_data['title'] = 'Title 2'
    post1_row = await post_table.create(post1_data)
    post2_row = await post_table.create(post2_data)

    rows = await post_table.list(limit=1, offset=0)
    assert len(rows) == 1
    assert rows[0]['title'] == post1_row['title']

    rows = await post_table.list(limit=1, offset=1)
    assert len(rows) == 1
    assert rows[0]['title'] == post2_row['title']


async def test_post_table_list_with_joins(post_table, comment_table, post_data, comment_data):
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


async def test_post_table_detail(post_table, post_data):
    created_row = await post_table.create(post_data)
    selected_row = await post_table.detail(created_row['id'])
    assert created_row['id'] == selected_row['id']
    assert selected_row['id']
    assert selected_row['title'] == post_data['title']
    assert selected_row['body'] == post_data['body']
    assert selected_row['pub_date'] == post_data['pub_date']
    assert post_table.hook_event_mock.pre_detail.called is True
    assert post_table.hook_event_mock.post_detail.called is True


async def test_post_table_detail_with_fields(post_table, post_data):
    created_row = await post_table.create(post_data)
    selected_row = await post_table.detail(created_row['id'], fields=['title'])
    assert created_row['title'] == selected_row['title']
    assert 'id' not in selected_row
    assert selected_row['title'] == post_data['title']
    assert 'body' not in selected_row
    assert 'pub_date' not in selected_row


async def test_post_table_detail_with_other_pk_field(post_table, post_data):
    created_row = await post_table.create(post_data)
    selected_row = await post_table.detail(created_row['title'], pk_field='title')
    assert created_row['id'] == selected_row['id']
    assert selected_row['id']
    assert selected_row['title'] == post_data['title']
    assert selected_row['body'] == post_data['body']
    assert selected_row['pub_date'] == post_data['pub_date']


async def test_post_table_update(post_table, post_data):
    row = await post_table.create(post_data)
    id = row['id']
    assert row['title'] == post_data['title']
    assert row['body'] == post_data['body']
    assert row['pub_date'] == post_data['pub_date']
    update_post_data = {
        'title': 'Post about John Doe',
        'pub_date': datetime(2000, 1, 1, 0, 0, 0)
    }
    row = await post_table.update(id, update_post_data)
    assert row['title'] == update_post_data['title']
    assert row['pub_date'] == update_post_data['pub_date']
    assert post_table.hook_event_mock.pre_update.called is True
    assert post_table.hook_event_mock.post_update.called is True


async def test_post_table_delete(post_table, post_data):
    row = await post_table.create(post_data)
    id = row['id']
    assert await post_table.delete(id) is True
    rows = await post_table.list()
    assert len(rows) == 0
    assert post_table.hook_event_mock.pre_delete.called is True
    assert post_table.hook_event_mock.post_delete.called is True


async def test_post_table_transaction(post_table, post_data):
    conn = await post_table.database.get_connection()
    with suppress(Exception):
        async with conn.transaction():
            await post_table.create(post_data, connection=conn, close_connection=False)
            raise Exception('BOOM')
    await conn.close()
    rows = await post_table.list()
    assert len(rows) == 0


async def test_pool_post_table_transaction(pool_post_table, post_data):
    await pool_post_table.database.init_pool()
    conn = await pool_post_table.database.get_connection()
    with suppress(Exception):
        async with conn.transaction():
            await pool_post_table.create(post_data, connection=conn, close_connection=False)
            raise Exception('BOOM')
    await pool_post_table.database.pool.release(conn)
    rows = await pool_post_table.list()
    assert len(rows) == 0


async def test_hook_trigger_invalid_event(post_table, post_data):
    hook = post_table.hooks[0]
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
async def test_post_table_parse_filters(filters, expected_result, post_table):
    result = post_table.parse_filters(filters)
    assert result == expected_result
