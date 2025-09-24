#!/usr/bin/env python3
"""
Enhanced ETL Pipeline dengan Data Governance & Quality
Path-aware version
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

# ==== Base Path Config ====
CURRENT_DIR = Path(__file__).parent
SCRIPTS_DIR = CURRENT_DIR.parent
ROOT_DIR = SCRIPTS_DIR.parent

# Tambahkan ke Python path biar import modul gampang
sys.path.append(str(SCRIPTS_DIR))
sys.path.append(str(ROOT_DIR))

# ==== Import ETL Modules ====
from scripts.etl_rakamin_kalbe.extract_rakamin_kalbe_v1_24092025_2035_ane import extract_multiple_tables
from scripts.etl_rakamin_kalbe.transform_rakamin_kalbe_v1_24092025_2036_ane import clean_customer_data, transform_orders, create_sales_summary
from scripts.etl_rakamin_kalbe.load_rakamin_kalbe_v1_24092025_2037_ane import load_to_sqlite, load_to_parquet

# ==== Governance & Lineage ====
from scripts.governance_rakamin_kalbe.metadata_manager_rakamin_kalbe_v1_24092025_2104_ane import MetadataManager, register_rakamin_assets
from scripts.governance_rakamin_kalbe.data_catalog_rakamin_kalbe_v1_24092025_ane import DataCatalog
from scripts.governance_rakamin_kalbe.linear_tracker_rakamin_kalbe_v1_24092025_ane import LineageTracker

# ==== Quality ====
from scripts.quality_rakamin_kalbe.data_quality_rakamin_kalbe_v1_24092025_ane import DataQualityChecker
from scripts.quality_rakamin_kalbe.quality_dashboard_rakamin_kalbe_v1_24092025_ane import QualityDashboard



class GovernedETLPipeline:
    def __init__(self):
        self.root_dir = ROOT_DIR
        self.setup_directories()
        self.setup_logging()

        self.metadata_manager = MetadataManager()
        self.data_catalog = DataCatalog()
        self.lineage_tracker = LineageTracker()
        self.quality_checker = DataQualityChecker()
        self.quality_results = []

    def setup_directories(self):
        """Setup semua folder penting (data, logs, reports, config)."""
        directories = [
            self.root_dir / "data" / "database",
            self.root_dir / "data" / "processed",
            self.root_dir / "logs",
            self.root_dir / "reports",
            self.root_dir / "config"
        ]
        for d in directories:
            d.mkdir(parents=True, exist_ok=True)

    def setup_logging(self):
        """Setup logging dengan file di /logs."""
        log_file = self.root_dir / "logs" / f"governed_etl_{datetime.now().strftime('%Y%m%d')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def run_pipeline(self):
        """Main governed pipeline execution"""
        logging.info("🚀 Starting Governed ETL Pipeline")

        DB_SOURCE = self.root_dir / "data" / "database" / "rakamin_kalbe.db"
        DB_TARGET = self.root_dir / "data" / "database" / "rakamin_kalbe_warehouse.db"

        try:
            # 1. Governance init
            self.initialize_governance()

            # 2. Extract
            raw_data = self.extract_phase(DB_SOURCE)

            # 3. Transform
            transformed_data = self.transform_phase(raw_data)

            # 4. Load
            self.load_phase(transformed_data, DB_TARGET)

            # 5. Reports
            self.reporting_phase()

            logging.info("✅ Governed ETL Pipeline Completed Successfully")

        except Exception as e:
            logging.error(f"❌ Pipeline failed: {e}")
            raise

    def initialize_governance(self):
        """Register metadata & log eksekusi pipeline"""
        logging.info("📊 Initializing Data Governance")
        register_rakamin_assets()
        self.lineage_tracker.log_transformation(
            source_table="rakamin_kalbe.db",
            target_table="etl_pipeline",
            transformation_type="pipeline_execution",
            records_in=0,
            records_out=0
        )

    def extract_phase(self, db_source):
        """Extract phase + lineage"""
        logging.info("🔍 Extraction Phase Started")
        tables = ["orders", "sales", "customer_data_history", "category_db"]
        raw_data = extract_multiple_tables(str(db_source), tables)

        for t, df in raw_data.items():
            self.lineage_tracker.log_transformation(
                source_table=f"{db_source.name}.{t}",
                target_table=f"staging.{t}",
                transformation_type="extraction",
                records_in=len(df),
                records_out=len(df)
            )
        return raw_data

    def transform_phase(self, raw_data):
        """Transform phase dengan quality checks"""
        logging.info("🔄 Transformation Phase Started")
        transformed_data = {}

        # Customers
        if "customer_data_history" in raw_data:
            df_customers = raw_data["customer_data_history"]

            # QC sebelum
            self.quality_results.append(
                self.quality_checker.run_all_checks(df_customers, "customers_raw")
            )

            df_customers_clean = clean_customer_data(df_customers)

            # QC sesudah
            self.quality_results.append(
                self.quality_checker.run_all_checks(df_customers_clean, "customers_clean")
            )

            transformed_data["dim_customers"] = df_customers_clean

            self.lineage_tracker.log_transformation(
                source_table="staging.customer_data_history",
                target_table="transformed.dim_customers",
                transformation_type="cleaning",
                records_in=len(df_customers),
                records_out=len(df_customers_clean)
            )

        # Orders
        if "orders" in raw_data and "customer_data_history" in raw_data:
            df_orders = transform_orders(raw_data["orders"], raw_data["customer_data_history"])

            self.quality_results.append(
                self.quality_checker.run_all_checks(df_orders, "fact_orders")
            )

            transformed_data["fact_orders"] = df_orders

            self.lineage_tracker.log_transformation(
                source_table="staging.orders + staging.customers",
                target_table="transformed.fact_orders",
                transformation_type="join_and_enrich",
                records_in=len(raw_data["orders"]),
                records_out=len(df_orders)
            )

        return transformed_data

    def load_phase(self, transformed_data, db_target):
        """Load ke warehouse + final QC"""
        logging.info("📤 Load Phase Started")
        for table, df in transformed_data.items():
            final_qc = self.quality_checker.run_all_checks(df, f"final_{table}")
            self.quality_results.append(final_qc)

            if final_qc["overall_status"] in ["PASS", "WARNING"]:
                load_to_sqlite(df, table, str(db_target))
                load_to_parquet(df, f"{table}.parquet")
                self.data_catalog.update_catalog(df, table, "ETL Pipeline")

                self.lineage_tracker.log_transformation(
                    source_table=f"transformed.{table}",
                    target_table=f"warehouse.{table}",
                    transformation_type="loading",
                    records_in=len(df),
                    records_out=len(df)
                )
            else:
                logging.error(f"❌ QC failed for {table}, skipping load")

    def reporting_phase(self):
        """Generate quality dashboard & summary"""
        logging.info("📈 Generating Reports")

        dashboard = QualityDashboard(self.quality_results)
        dashboard.create_quality_visualization()

        summary = dashboard.generate_quality_report()
        print("\n" + "=" * 60)
        print("DATA QUALITY SUMMARY")
        print("=" * 60)
        print(summary.to_string(index=False))

        self.lineage_tracker.log_transformation(
            source_table="etl_pipeline",
            target_table="reporting",
            transformation_type="reporting",
            records_in=0,
            records_out=len(self.quality_results)
        )


if __name__ == "__main__":
    pipeline = GovernedETLPipeline()
    pipeline.run_pipeline()
