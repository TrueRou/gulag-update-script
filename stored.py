import aiomysql
from aiomysql import Connection, DictCursor, Pool
from contextlib import asynccontextmanager

import config

source_pool: Pool = None
target_pool: Pool = None


@asynccontextmanager
async def db_context(thePool: Pool) -> (Connection, DictCursor):
    try:
        conn: Connection = await thePool.acquire()
        cur: DictCursor = await conn.cursor(DictCursor)
        yield conn, cur
    finally:
        await cur.close()
        await thePool.release(conn)


async def create_pool():
    global source_pool, target_pool
    if source_pool is not None and target_pool is not None:
        return
    source_pool = await aiomysql.create_pool(host=config.source_mysql_host, port=3306,
                                             user=config.source_mysql_user, password=config.source_mysql_password,
                                             db=config.source_mysql_dbname, charset='utf8')
    target_pool = await aiomysql.create_pool(host=config.target_mysql_host, port=3306,
                                             user=config.target_mysql_user, password=config.target_mysql_password,
                                             db=config.target_mysql_dbname, charset='utf8', autocommit=True)
