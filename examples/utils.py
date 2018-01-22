async def create_table(database):
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


async def drop_table(database):
    conn = await database.get_connection()
    await conn.execute(
        """
        DROP TABLE users
        """
    )
    await conn.close()
    return True
