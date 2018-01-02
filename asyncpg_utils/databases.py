import abc

import asyncpg


class AbstractDatabase:
    @abc.abstractmethod
    async def get_connection(self):
        """A coroutine that returns a connection to database."""

    @abc.abstractmethod
    async def call_connection_coroutine(self, coroutine_name, sql_query, *args, connection=None, timeout=None):
        """A coroutine that executes another coroutine inside connection."""

    async def query(self, sql_query, *args, connection=None, timeout=None):
        return await self.call_connection_coroutine('fetch', sql_query, *args, connection=connection, timeout=timeout)

    async def query_one(self, sql_query, *args, connection=None, timeout=None):
        return await self.call_connection_coroutine('fetchrow', sql_query, *args, connection=connection, timeout=timeout)

    async def insert(self, table_name, data, connection=None, timeout=None):
        fields = [field_name for field_name in data.keys()]
        values = [field_value for field_name, field_value in data.items()]
        variables = tuple('${}'.format(x + 1) for x in range(len(fields)))
        sql_query = (
            'INSERT INTO {table_name} '
            '({fields}) '
            'VALUES ({values}) '
            'RETURNING *'
        ).format(
            table_name=table_name,
            fields=', '.join('"{}"'.format(field) for field in fields),
            values=', '.join(variables),
        )
        return await self.call_connection_coroutine('fetchrow', sql_query, *values, connection=connection, timeout=timeout)


class Database(AbstractDatabase):
    def __init__(self, dsn, **kwargs):
        self.dsn = dsn
        self.kwargs = kwargs

    async def get_connection(self):
        return await asyncpg.connect(self.dsn, **self.kwargs)

    async def call_connection_coroutine(self, coroutine_name, sql_query, *args, connection=None, timeout=None):
        conn = connection or await self.get_connection()
        connection_coroutine = getattr(conn, coroutine_name)
        try:
            return await connection_coroutine(sql_query, *args, timeout=timeout)
        finally:
            await conn.close()


class PoolDatabase(AbstractDatabase):
    def __init__(self, dsn, pool=None, **kwargs):
        self.dsn = dsn
        self.pool = pool
        self.kwargs = kwargs

    async def get_connection(self):
        if self.pool is None:
            self.pool = await asyncpg.create_pool(self.dsn, **self.kwargs)

        return await self.pool.acquire()

    async def call_connection_coroutine(self, coroutine_name, sql_query, *args, connection=None, timeout=None):
        conn = connection or await self.get_connection()
        connection_coroutine = getattr(conn, coroutine_name)
        try:
            return await connection_coroutine(sql_query, *args, timeout=timeout)
        finally:
            await self.pool.release(conn)
