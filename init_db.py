from database.connection import get_connection
from utils.logger import get_logger
import os

logger = get_logger(__name__)


def init_db():
    sql_path = os.path.join('sql', 'schema.sql')
    logger.info('Initialising database...')
    conn = get_connection()
    cursor = conn.cursor()
    with open(sql_path, 'r') as f:
        schema = f.read()
    cursor.execute(schema)
    conn.commit()
    cursor.close()
    conn.close()
    logger.info('Schema created successfully.')


if __name__ == '__main__':
    init_db()
