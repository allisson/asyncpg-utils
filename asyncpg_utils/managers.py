from pypika import PostgreSQLQuery as Query, Table


class TableManager:
    def __init__(self, database, table_name, pk_field='id'):
        self.database = database
        self.table_name = table_name
        self.pk_field = pk_field
        self._table = Table(self.table_name)

    async def list(self, fields=None, filters=None):
        filters = filters or {}
        sql_query = Query.from_(self._table).select(*fields or '*')
        for field, value in filters.items():
            sql_query = sql_query.where(getattr(self._table, field) == value)
        return await self.database.query(str(sql_query))

    async def detail(self, pk, fields=None):
        sql_query = Query.from_(self._table).select(*fields or '*').where(getattr(self._table, self.pk_field) == pk)
        return await self.database.query_one(str(sql_query))

    async def create(self, data):
        field_names = [field_name for field_name in data.keys()]
        field_values = [field_value for _, field_value in data.items()]
        sql_query = Query.into(self._table).columns(*field_names).insert(*field_values)
        sql_query = '{} RETURNING *'.format(str(sql_query))
        return await self.database.query_one(sql_query)

    async def update(self, pk, data):
        sql_query = Query.update(self._table).where(getattr(self._table, self.pk_field) == pk)
        for field, value in data.items():
            sql_query = sql_query.set(field, value)
        sql_query = '{} RETURNING *'.format(str(sql_query))
        return await self.database.query_one(sql_query)

    async def delete(self, pk):
        sql_query = Query.from_(self._table).where(getattr(self._table, self.pk_field) == pk).delete()
        await self.database.query_one(str(sql_query))
        return True
