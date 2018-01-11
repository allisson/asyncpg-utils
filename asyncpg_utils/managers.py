from .templates import (
    sql_create_template,
    sql_delete_template,
    sql_detail_template,
    sql_list_template,
    sql_update_template,
)


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

    async def trigger_hooks(self, event_name, *args, **kwargs):
        for hook in self.hooks:
            await hook.trigger_event(event_name, *args, **kwargs)

    async def create(self, data):
        field_names = [field_name for field_name in data.keys()]
        field_values = [field_value for _, field_value in data.items()]
        sql_query = sql_create_template.render({
            'table_name': self.table_name,
            'field_names': field_names
        })
        await self.trigger_hooks('pre_create', data)
        row = await self.database.query_one(sql_query, *field_values)
        await self.trigger_hooks('post_create', row)
        return row

    async def list(self, fields=None, filters=None, order_by=None, order_by_sort='ASC'):
        filters = filters or {}
        filter_values = [filter_value for _, filter_value in filters.items()]
        sql_query = sql_list_template.render({
            'table_name': self.table_name,
            'fields': fields,
            'filters': filters,
            'order_by': order_by,
            'order_by_sort': order_by_sort
        })
        await self.trigger_hooks('pre_list', fields, filters, order_by, order_by_sort)
        rows = await self.database.query(sql_query, *filter_values)
        await self.trigger_hooks('post_list', rows)
        return rows

    async def detail(self, pk, fields=None):
        sql_query = sql_detail_template.render({
            'table_name': self.table_name,
            'fields': fields,
            'pk_field': self.pk_field
        })
        await self.trigger_hooks('pre_detail', pk, fields)
        row = await self.database.query_one(sql_query, pk)
        await self.trigger_hooks('post_detail', row)
        return row

    async def update(self, pk, data):
        field_names = [field_name for field_name in data.keys()]
        field_values = [field_value for _, field_value in data.items()]
        sql_query = sql_update_template.render({
            'table_name': self.table_name,
            'field_names': field_names,
            'pk_field': self.pk_field
        })
        await self.trigger_hooks('pre_update', pk, data)
        row = await self.database.query_one(sql_query, *field_values, pk)
        await self.trigger_hooks('post_update', row)
        return row

    async def delete(self, pk):
        sql_query = sql_delete_template.render({
            'table_name': self.table_name,
            'pk_field': self.pk_field
        })
        await self.trigger_hooks('pre_delete', pk)
        await self.database.query_one(sql_query, pk)
        await self.trigger_hooks('post_delete', pk)
        return True
