# scripts/query.py
import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def query_sqlite(db_path: str, sql: str) -> pd.DataFrame:
    """Run query against a SQLite database and return a DataFrame."""
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(sql, conn)
        conn.close()
        logger.info(f"Query executed successfully on {db_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to execute query on {db_path}: {e}")
        raise

def query_postgres(conn_str: str, sql: str) -> pd.DataFrame:
    """Run query against a Postgres database using SQLAlchemy."""
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        logger.info("Query executed successfully on Postgres")
        return df
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise
