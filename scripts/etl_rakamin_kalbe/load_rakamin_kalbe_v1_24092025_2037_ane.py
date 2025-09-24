import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import logging
import os
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DB_DEV_DIR = DATA_DIR / "database"
DB_DIR = DB_DEV_DIR / "dev"
PROCESSED_DIR = DATA_DIR / "processed"

def get_db_path(db_name):
    """Get absolute path untuk database"""
    return DB_DIR / db_name

def get_processed_path(filename):
    """Get absolute path untuk processed data"""
    return PROCESSED_DIR / filename

def load_to_sqlite(df, table_name, db_name, if_exists='replace'):
    """
    Load DataFrame ke SQLite database
    """
    try:
        db_path = get_db_path(db_name)
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        conn.close()
        logging.info(f"Berhasil load {len(df)} rows ke {db_name}.{table_name}")
        return True
    except Exception as e:
        logging.error(f"Error load data ke {db_name}.{table_name}: {e}")
        return False

def load_to_parquet(df, filename):
    """
    Save DataFrame ke Parquet format
    """
    try:
        file_path = get_processed_path(filename)
        df.to_parquet(file_path, index=False)
        logging.info(f"Berhasil save ke {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error save parquet: {e}")
        return False

def load_to_csv(df, filename):
    """
    Save DataFrame ke CSV
    """
    try:
        file_path = get_processed_path(filename)
        df.to_csv(file_path, index=False)
        logging.info(f"Berhasil save ke {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error save CSV: {e}")
        return False