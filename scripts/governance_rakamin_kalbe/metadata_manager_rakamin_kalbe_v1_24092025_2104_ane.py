import pandas as pd
import sqlite3
import json
from datetime import datetime
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent  # Root project directory
DATA_DIR = BASE_DIR / "data"
DB_DEV_DIR = DATA_DIR / "database"
DB_DIR = DB_DEV_DIR / "dev"

class MetadataManager:
    def __init__(self, db_name="metadata_catalog.db"):
        self.db_path = DB_DIR / db_name
        self.init_metadata_db()
    
    def init_metadata_db(self):
        """Initialize metadata database dengan struktur tabel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table untuk data assets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_assets (
                asset_id TEXT PRIMARY KEY,
                asset_name TEXT,
                asset_type TEXT,
                source_system TEXT,
                owner TEXT,
                sensitivity_level TEXT,
                created_date TIMESTAMP,
                last_updated TIMESTAMP,
                data_classification TEXT,
                retention_days INTEGER
            )
        ''')
        
        # Table untuk data lineage
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_lineage (
                lineage_id TEXT PRIMARY KEY,
                source_asset_id TEXT,
                target_asset_id TEXT,
                transformation_logic TEXT,
                executed_at TIMESTAMP,
                records_processed INTEGER,
                status TEXT,
                FOREIGN KEY (source_asset_id) REFERENCES data_assets (asset_id),
                FOREIGN KEY (target_asset_id) REFERENCES data_assets (asset_id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def register_data_asset(self, asset_data: dict):
        """Register new data asset ke metadata db"""
        conn = sqlite3.connect(self.db_path)
        asset_data['created_date'] = datetime.now()
        asset_data['last_updated'] = datetime.now()
        
        df = pd.DataFrame([asset_data])
        df.to_sql('data_assets', conn, if_exists='append', index=False)
        conn.close()
        
        print(f"Registered asset: {asset_data['asset_name']}")

def register_rakamin_assets(db_name="metadata_catalog.db"):
    """Register sample Rakamin assets"""
    manager = MetadataManager(db_name)
    
    assets = [
        {
            'asset_id': 'rakamin_orders_raw',
            'asset_name': 'Orders Raw Data',
            'asset_type': 'table',
            'source_system': 'rakamin_kalbe.db',
            'owner': 'data_team',
            'sensitivity_level': 'confidential',
            'data_classification': 'PII',
            'retention_days': 365
        },
        {
            'asset_id': 'dim_customers_clean',
            'asset_name': 'Customers Dimension',
            'asset_type': 'table', 
            'source_system': 'ETL Pipeline',
            'owner': 'analytics_team',
            'sensitivity_level': 'restricted',
            'data_classification': 'PII',
            'retention_days': 730
        }
    ]
    
    for asset in assets:
        manager.register_data_asset(asset)
