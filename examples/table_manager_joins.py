import asyncio
from datetime import datetime

from asyncpg_utils.databases import Database
from asyncpg_utils.managers import TableManager

loop = asyncio.get_event_loop()
database = Database('postgresql://postgres:postgres@localhost/asyncpg-utils')
post_table_manager = TableManager(database, 'posts')
comment_table_manager = TableManager(database, 'comments')
post_data = {
    'title': 'Post Title',
    'body': 'Post Body',
    'pub_date': datetime.utcnow()
}
comment_data = {
    'body': 'Comment Body',
    'pub_date': datetime.utcnow()
}


async def create_table(database):
    conn = await database.get_connection()
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posts(
            id serial PRIMARY KEY,
            title varchar(128),
            body text,
            pub_date timestamp
        );
        CREATE TABLE IF NOT EXISTS comments(
            id serial PRIMARY KEY,
            post_id integer references posts(id),
            body text,
            pub_date timestamp
        );
        """
    )
    await conn.close()
    return True


async def drop_table(database):
    conn = await database.get_connection()
    await conn.execute(
        """
        DROP TABLE comments;
        DROP TABLE posts;
        """
    )
    await conn.close()
    return True


async def main():
    await create_table(database)
    post_row = await post_table_manager.create(post_data)
    comment_data['post_id'] = post_row['id']
    await comment_table_manager.create(comment_data)
    fields = (
        'comments.id',
        'comments.body',
        'comments.pub_date',
        'posts.title as post_title',
        'posts.body as post_body',
        'posts.pub_date as post_pub_date'
    )
    joins = {'posts': {'type': 'INNER JOIN', 'source': 'comments.post_id', 'target': 'posts.id'}}
    rows = await comment_table_manager.list(fields=fields, joins=joins)
    print('comment_table_manager.list, inner_join_posts, rows={!r}'.format(rows))
    await drop_table(database)

loop.run_until_complete(main())
