import pandas as pd
import sqlite3
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DB_DEV_DIR = DATA_DIR / "database"
DB_DIR = DB_DEV_DIR / "dev"
DOCS_DIR = BASE_DIR / "docs"

class DataCatalog:
    def __init__(self, catalog_db_name="metadata_catalog.db"):
        self.catalog_db_path = DB_DIR / catalog_db_name
        self.docs_dir = DOCS_DIR

    def generate_data_dictionary(self, df, table_name, description=""):
        """Generate data dictionary untuk sebuah tabel"""
        data_dict = []
        
        for column in df.columns:
            data_dict.append({
                "table_name": table_name,
                "column_name": column,
                "data_type": str(df[column].dtype),
                "sample_data": str(df[column].iloc[0]) if len(df) > 0 else "NULL",
                "null_count": df[column].isnull().sum(),
                "unique_count": df[column].nunique(),
                "description": description,
                "generated_at": pd.Timestamp.now()
            })
        
        return pd.DataFrame(data_dict)

    def update_catalog(self, df, table_name, source_system, description=""):
        """Update data catalog dengan metadata terbaru"""
        data_dict_df = self.generate_data_dictionary(df, table_name, description)
        
        # Save to CSV catalog di folder docs
        catalog_path = self.docs_dir / f"data_catalog_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
        data_dict_df.to_csv(catalog_path, index=False)
        
        # Update database catalog
        conn = sqlite3.connect(self.catalog_db_path)
        data_dict_df.to_sql("data_dictionary", conn, if_exists="append", index=False)
        conn.close()
