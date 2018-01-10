from pypika import PostgreSQLQuery as Query, Table


class AbstractHook:
    def __init__(self, table_manager):
        self.table_manager = table_manager

    async def trigger_event(self, event_name, *args, **kwargs):
        event_coroutine = getattr(self, event_name, None)
        if event_coroutine is None:
            return
        return await event_coroutine(*args, **kwargs)


class TableManager:
    def __init__(self, database, table_name, pk_field='id', hooks=None):
        self.database = database
        self.table_name = table_name
        self.pk_field = pk_field
        self.hooks = [hook(self) for hook in hooks or []]
        self._table = Table(self.table_name)

    async def trigger_hooks(self, event_name, *args, **kwargs):
        for hook in self.hooks:
            await hook.trigger_event(event_name, *args, **kwargs)

    async def create(self, data):
        field_names = [field_name for field_name in data.keys()]
        field_values = [field_value for _, field_value in data.items()]
        sql_query = Query.into(self._table).columns(*field_names).insert(*field_values)
        sql_query = '{} RETURNING *'.format(str(sql_query))
        await self.trigger_hooks('pre_create', data)
        row = await self.database.query_one(sql_query)
        await self.trigger_hooks('post_create', row)
        return row

    async def list(self, fields=None, filters=None):
        filters = filters or {}
        sql_query = Query.from_(self._table).select(*fields or '*')
        for field, value in filters.items():
            sql_query = sql_query.where(getattr(self._table, field) == value)
        await self.trigger_hooks('pre_list', fields, filters)
        rows = await self.database.query(str(sql_query))
        await self.trigger_hooks('post_list', rows)
        return rows

    async def detail(self, pk, fields=None):
        sql_query = Query.from_(self._table).select(*fields or '*').where(getattr(self._table, self.pk_field) == pk)
        await self.trigger_hooks('pre_detail', pk, fields)
        row = await self.database.query_one(str(sql_query))
        await self.trigger_hooks('post_detail', row)
        return row

    async def update(self, pk, data):
        sql_query = Query.update(self._table).where(getattr(self._table, self.pk_field) == pk)
        for field, value in data.items():
            sql_query = sql_query.set(field, value)
        sql_query = '{} RETURNING *'.format(str(sql_query))
        await self.trigger_hooks('pre_update', pk, data)
        row = await self.database.query_one(sql_query)
        await self.trigger_hooks('post_update', row)
        return row

    async def delete(self, pk):
        sql_query = Query.from_(self._table).where(getattr(self._table, self.pk_field) == pk).delete()
        await self.trigger_hooks('pre_delete', pk)
        await self.database.query_one(str(sql_query))
        await self.trigger_hooks('post_delete', pk)
        return True
