import os
from contextlib import contextmanager

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()

DATABASE_URL = os.getenv('DB_PROD_URL')

@contextmanager
def get_sync_db():
    """
    psycopg3 synchronous DB connection for Celery workers (Async should not be used).
    Always returns row dicts instead of tuples.
    """
    conn = psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row
    )
    try:
        yield conn
    finally:
        conn.close()