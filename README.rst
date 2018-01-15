=============
asyncpg-utils
=============

|TravisCI Build Status| |Coverage Status| |Requirements Status| |Version|

----

Utilities for Asyncpg.


How to install
==============

.. code:: shell

    pip install asyncpg-utils


How to Use
==========

Database
--------

.. code:: python

    # database.py
    import asyncio
    from datetime import date

    from asyncpg_utils.databases import Database

    loop = asyncio.get_event_loop()
    database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')


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


    async def insert_row(data):
        return await database.insert('users', data)


    async def query_all():
        return await database.query(
            """
            SELECT * FROM users
            """
        )


    async def query_one():
        return await database.query_one(
            """
            SELECT * FROM users
            WHERE name = $1
            """,
            'Jane Doe'
        )


    async def main():
        print('create_table users, {!r}'.format(await create_table()))
        print('insert row, {!r}'.format(await insert_row({'name': 'John Doe', 'dob': date(2000, 1, 1)})))
        print('insert row, {!r}'.format(await insert_row({'name': 'Jane Doe', 'dob': date(2000, 1, 1)})))
        print('query all results, {!r}'.format(await query_all()))
        print('query one result, {!r}'.format(await query_one()))

    loop.run_until_complete(main())

.. code:: shell
    
    # python database.py
    create_table users, True
    insert row, <Record id=1 name='John Doe' dob=datetime.date(2000, 1, 1)>
    insert row, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>
    query all results, [<Record id=1 name='John Doe' dob=datetime.date(2000, 1, 1)>, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>]
    query one result, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>

PoolDatabase
------------

.. code:: python

    # database_pool.py
    import asyncio
    from datetime import date

    from asyncpg_utils.databases import PoolDatabase

    loop = asyncio.get_event_loop()
    database = PoolDatabase('postgresql://postgres:postgres@localhost/asyncpg-utils')


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


    async def insert_row(data):
        return await database.insert('users', data)


    async def query_all():
        return await database.query(
            """
            SELECT * FROM users
            """
        )


    async def query_one():
        return await database.query_one(
            """
            SELECT * FROM users
            WHERE name = $1
            """,
            'Jane Doe'
        )


    async def main():
        print('create_table users, {!r}'.format(await create_table()))
        print('insert row, {!r}'.format(await insert_row({'name': 'John Doe', 'dob': date(2000, 1, 1)})))
        print('insert row, {!r}'.format(await insert_row({'name': 'Jane Doe', 'dob': date(2000, 1, 1)})))
        print('query all results, {!r}'.format(await query_all()))
        print('query one result, {!r}'.format(await query_one()))

    loop.run_until_complete(main())

.. code:: shell
    
    # python database_pool.py
    create_table users, True
    insert row, <Record id=1 name='John Doe' dob=datetime.date(2000, 1, 1)>
    insert row, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>
    query all results, [<Record id=1 name='John Doe' dob=datetime.date(2000, 1, 1)>, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>]
    query one result, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>

TableManager
------------

.. code:: python

    # table_manager.py
    import asyncio
    from datetime import date

    from asyncpg_utils.databases import Database
    from asyncpg_utils.managers import TableManager

    loop = asyncio.get_event_loop()
    database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
    table_manager = TableManager(database, 'users', pk_field='id', hooks=None)
    user_data = {
        'name': 'Allisson',
        'dob': date(1983, 2, 9)
    }


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


    async def table_manager_create():
        print('table_manager.create, row={!r}'.format(await table_manager.create(user_data)))


    async def table_manager_list():
        print('table_manager.list, rows={!r}'.format(await table_manager.list()))
        print('table_manager.list, only_name_field, rows={!r}'.format(await table_manager.list(fields=['name'])))
        print('table_manager.list, filter_by_id, rows={!r}'.format(await table_manager.list(filters={'id': 999999})))
        print('table_manager.list, order_by=name, order_by_sort=ASC, rows={!r}'.format(await table_manager.list(order_by='name', order_by_sort='ASC')))
        print('table_manager.list, count=True, rows={!r}'.format(await table_manager.list(count=True)))
        print('table_manager.list, limit=1, offset=0, rows={!r}'.format(await table_manager.list(limit=1, offset=0)))


    async def table_manager_detail():
        print('table_manager.detail, row={!r}'.format(await table_manager.detail(1)))
        print('table_manager.detail, only_name_field, row={!r}'.format(await table_manager.detail(1, fields=['name'])))


    async def table_manager_update():
        user_data['name'] = 'John Doe'
        print('table_manager.update, row={!r}'.format(await table_manager.update(1, user_data)))


    async def table_manager_delete():
        print('table_manager.delete, result={!r}'.format(await table_manager.delete(1)))


    async def main():
        print('create_table users, {!r}'.format(await create_table()))
        await table_manager_create()
        await table_manager_list()
        await table_manager_detail()
        await table_manager_update()
        await table_manager_delete()

    loop.run_until_complete(main())

.. code:: shell
    
    # python table_manager.py
    create_table users, True
    table_manager.create, row=<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>
    table_manager.list, rows=[<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>]
    table_manager.list, only_name_field, rows=[<Record name='Allisson'>]
    table_manager.list, filter_by_id, rows=[]
    table_manager.list, order_by=name, order_by_sort=ASC, rows=[<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>]
    table_manager.list, count=True, rows=[<Record count=1>]
    table_manager.list, limit=1, offset=0, rows=[<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>]
    table_manager.detail, row=<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>
    table_manager.detail, only_name_field, row=<Record name='Allisson'>
    table_manager.update, row=<Record id=1 name='John Doe' dob=datetime.date(1983, 2, 9)>
    table_manager.delete, result=True

Table Manager Hook
------------------

.. code:: python

    # table_manager_hook.py
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

        async def pre_list(self, fields, filters, order_by, order_by_sort, count, limit, offset):
            print('pre_list, fields={!r}, filters={!r}, order_by={!r}, order_by_sort={!r}, count={!r}, limit={!r}, offset={!r}'.format(fields, filters, order_by, order_by_sort, count, limit, offset))

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

.. code:: shell
    
    # python table_manager_hook.py
    create_table users, True
    pre_create, data={'name': 'Allisson', 'dob': datetime.date(1983, 2, 9)}
    post_create, row=<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>
    pre_list, fields=None, filters={}, order_by=None, order_by_sort='ASC', count=False, limit=None, offset=None
    post_list, rows=[<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>]
    pre_detail, pk=1, pk_field='id', fields=None
    post_detail, row=<Record id=1 name='Allisson' dob=datetime.date(1983, 2, 9)>
    pre_update, pk=1, data={'name': 'John Doe', 'dob': datetime.date(1983, 2, 9)}
    post_update, row=<Record id=1 name='John Doe' dob=datetime.date(1983, 2, 9)>
    pre_delete, pk=1
    post_delete, pk=1

Check `https://github.com/allisson/asyncpg-utils/tree/master/examples <https://github.com/allisson/asyncpg-utils/tree/master/examples>`_ for more code examples.

.. |TravisCI Build Status| image:: https://travis-ci.org/allisson/asyncpg-utils.svg?branch=master
   :target: https://travis-ci.org/allisson/asyncpg-utils
.. |Coverage Status| image:: https://codecov.io/gh/allisson/asyncpg-utils/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/allisson/asyncpg-utils
.. |Requirements Status| image:: https://requires.io/github/allisson/asyncpg-utils/requirements.svg?branch=master
   :target: https://requires.io/github/allisson/asyncpg-utils/requirements/?branch=master
.. |Version| image:: https://badge.fury.io/py/asyncpg-utils.svg
    :target: https://badge.fury.io/py/asyncpg-utils
