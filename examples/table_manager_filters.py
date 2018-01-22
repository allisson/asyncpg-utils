import asyncio
from datetime import date

from asyncpg_utils.databases import Database
from asyncpg_utils.managers import TableManager

from utils import create_table, drop_table

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
table_manager = TableManager(database, 'users', pk_field='id', hooks=None)
user1_data = {
    'name': 'Allisson',
    'dob': date(1983, 2, 9)
}
user2_data = {
    'name': 'John Doe',
    'dob': date(2000, 1, 1)
}


async def main():
    await create_table(database)
    user1_row = await table_manager.create(user1_data)
    user2_row = await table_manager.create(user2_data)
    print('table_manager.list, filter_by_name, rows={!r}'.format(await table_manager.list(filters={'name': user1_row['name']})))
    print('table_manager.list, filter_by_name__exact, rows={!r}'.format(await table_manager.list(filters={'name__exact': user1_row['name']})))
    print('table_manager.list, filter_by_name__like, rows={!r}'.format(await table_manager.list(filters={'name__like': '%Doe%'})))
    print('table_manager.list, filter_by_name__ilike, rows={!r}'.format(await table_manager.list(filters={'name__ilike': '%doe%'})))
    print('table_manager.list, filter_by_id__in, rows={!r}'.format(await table_manager.list(filters={'id__in': [user1_row['id'], user2_row['id']]})))
    print('table_manager.list, filter_by_dob__gt, rows={!r}'.format(await table_manager.list(filters={'dob__gt': user1_row['dob']})))
    print('table_manager.list, filter_by_dob__gte, rows={!r}'.format(await table_manager.list(filters={'dob__gte': user1_row['dob']})))
    print('table_manager.list, filter_by_dob__lt, rows={!r}'.format(await table_manager.list(filters={'dob__lt': user2_row['dob']})))
    print('table_manager.list, filter_by_dob__lte, rows={!r}'.format(await table_manager.list(filters={'dob__lte': user2_row['dob']})))
    print('table_manager.list, filter_by_name_and_dob, rows={!r}'.format(await table_manager.list(filters={'name': user1_row['name'], 'dob': user1_row['dob']}, filters_operator='AND')))
    print('table_manager.list, filter_by_name_or_dob, rows={!r}'.format(await table_manager.list(filters={'name': user1_row['name'], 'dob': user2_row['dob']}, filters_operator='OR')))
    await drop_table(database)

loop.run_until_complete(main())
