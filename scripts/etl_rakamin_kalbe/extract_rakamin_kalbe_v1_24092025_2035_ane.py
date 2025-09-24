import sqlite3
import pandas as pd
import logging
import os
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent  # Root project directory
DATA_DIR = BASE_DIR / "data"
DB_DEV_DIR = DATA_DIR / "database"
DB_DIR = DB_DEV_DIR / "dev"

def get_db_path(db_name):
    """Get absolute path untuk database"""
    return DB_DIR / db_name

def extract_data(db_name, table_name):
    """
    Ekstrak data dari tabel SQLite ke DataFrame
    """
    try:
        db_path = get_db_path(db_name)
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logging.info(f"Berhasil ekstrak {len(df)} rows dari {table_name}")
        return df
    except Exception as e:
        logging.error(f"Error ekstrak data dari {db_name}.{table_name}: {e}")
        return None

def extract_multiple_tables(db_name, table_list):
    """
    Ekstrak multiple tables sekaligus
    """
    dataframes = {}
    for table in table_list:
        df = extract_data(db_name, table)
        if df is not None:
            dataframes[table] = df
    
    return dataframes