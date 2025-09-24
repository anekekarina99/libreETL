import json
from datetime import datetime
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent
LOGS_DIR = BASE_DIR / "logs"

class LineageTracker:
    def __init__(self, lineage_file="data_lineage.json"):
        self.lineage_file = LOGS_DIR / lineage_file
        self.lineage_file.parent.mkdir(exist_ok=True)

    def log_transformation(self, source_table, target_table, transformation_type, records_in, records_out, sql_query=None):
        """Log setiap transformasi yang dilakukan"""
        lineage_entry = {
            "timestamp": datetime.now().isoformat(),
            "source": source_table,
            "target": target_table,
            "transformation_type": transformation_type,
            "records_input": records_in,
            "records_output": records_out,
            "success_rate": round((records_out / records_in) * 100, 2) if records_in > 0 else 100,
            "sql_query": sql_query,
            "pipeline_version": "rakamin_v1_22092025"
        }

        # Load existing lineage
        if self.lineage_file.exists():
            with open(self.lineage_file, "r") as f:
                data = json.load(f)
        else:
            data = {"lineage_entries": []}

        data["lineage_entries"].append(lineage_entry)

        # Save updated lineage
        with open(self.lineage_file, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Lineage logged: {source_table} → {target_table}")
