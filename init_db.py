import asyncio
import os
from database.connection import get_connection
from utils.logger import get_logger

logger = get_logger(__name__)


async def init_db():
    sql_path = os.path.join('sql', 'schema.sql')
    logger.info('Initialising database...')

    conn = await get_connection()
    with open(sql_path, 'r') as f:
        schema = f.read()

    await conn.execute(schema)
    await conn.close()

    logger.info('Schema created successfully.')


if __name__ == '__main__':
    asyncio.run(init_db())