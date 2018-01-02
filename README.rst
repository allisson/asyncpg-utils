asyncpg-utils
=============

|TravisCI Build Status| |Coverage Status| |Requirements Status|

----

Utilities for Asyncpg.


How to install
--------------

.. code:: shell

    pip install asyncpg-utils


How to Use
----------

**Python code:**

.. code:: python

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

**Code executed:**

.. code:: shell

    create_table users, True
    insert row, <Record id=1 name='John Doe' dob=datetime.date(2000, 1, 1)>
    insert row, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>
    query all results, [<Record id=1 name='John Doe' dob=datetime.date(2000, 1, 1)>, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>]
    query one result, <Record id=2 name='Jane Doe' dob=datetime.date(2000, 1, 1)>

Check `https://github.com/allisson/asyncpg-utils/tree/master/examples <https://github.com/allisson/asyncpg-utils/tree/master/examples>`_ for more code examples.


.. |TravisCI Build Status| image:: https://travis-ci.org/allisson/asyncpg-utils.svg?branch=master
   :target: https://travis-ci.org/allisson/asyncpg-utils
.. |Coverage Status| image:: https://codecov.io/gh/allisson/asyncpg-utils/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/allisson/asyncpg-utils
.. |Requirements Status| image:: https://requires.io/github/allisson/asyncpg-utils/requirements.svg?branch=master
   :target: https://requires.io/github/allisson/asyncpg-utils/requirements/?branch=master
