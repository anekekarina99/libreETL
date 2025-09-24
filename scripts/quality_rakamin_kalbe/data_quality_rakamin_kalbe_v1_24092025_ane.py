import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = BASE_DIR / "config"

class DataQualityFramework:
    def __init__(self, rules_config="quality_rules.json"):
        self.rules_config = CONFIG_DIR / rules_config
        self.quality_results = []

    def load_quality_rules(self):
        """Load quality rules from JSON config"""
        try:
            with open(self.rules_config, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.warning(f"Rules config not found at {self.rules_config}, using default rules.")
            return self.get_default_rules()

    def get_default_rules(self):
        """Default quality rules untuk project rakamin"""
        return {
            "completeness_threshold": 0.95,
            "accuracy_threshold": 0.98,
            "consistency_rules": {
                "date_format": "YYYY-MM-DD",
                "currency_format": "IDR"
            },
            "table_specific_rules": {
                "orders": {
                    "required_columns": ["order_id", "customer_id", "order_date", "amount"],
                    "unique_columns": ["order_id"],
                    "not_null_columns": ["order_id", "customer_id"],
                    "value_ranges": {
                        "amount": {"min": 0, "max": 100000000},
                        "quantity": {"min": 1, "max": 1000}
                    }
                },
                "customers": {
                    "required_columns": ["customer_id", "customer_name", "email"],
                    "unique_columns": ["customer_id"],
                    "not_null_columns": ["customer_id", "customer_name"],
                    "format_rules": {
                        "email": "email_format"
                    }
                }
            }
        }


class DataQualityChecker(DataQualityFramework):
    def check_completeness(self, df, table_name):
        """Check data completeness"""
        rules = self.load_quality_rules()
        table_rules = rules['table_specific_rules'].get(table_name, {})

        results = {}
        for column in table_rules.get('not_null_columns', []):
            if column in df.columns:
                null_count = df[column].isnull().sum()
                completeness = 1 - (null_count / len(df)) if len(df) > 0 else 1
                results[f'completeness_{column}'] = {
                    'metric': 'completeness',
                    'column': column,
                    'null_count': null_count,
                    'completeness_rate': completeness,
                    'threshold': rules['completeness_threshold'],
                    'passed': completeness >= rules['completeness_threshold']
                }
        return results

    def check_uniqueness(self, df, table_name):
        """Check data uniqueness"""
        rules = self.load_quality_rules()
        table_rules = rules['table_specific_rules'].get(table_name, {})

        results = {}
        for column in table_rules.get('unique_columns', []):
            if column in df.columns:
                unique_count = df[column].nunique(dropna=True)
                duplicate_count = len(df) - unique_count
                uniqueness = unique_count / len(df) if len(df) > 0 else 1
                results[f'uniqueness_{column}'] = {
                    'metric': 'uniqueness',
                    'column': column,
                    'unique_count': unique_count,
                    'duplicate_count': duplicate_count,
                    'uniqueness_rate': uniqueness,
                    'passed': duplicate_count == 0
                }
        return results

    def check_accuracy(self, df, table_name):
        """Check data accuracy berdasarkan business rules"""
        rules = self.load_quality_rules()
        table_rules = rules['table_specific_rules'].get(table_name, {})

        results = {}
        value_ranges = table_rules.get('value_ranges', {})
        for column, range_config in value_ranges.items():
            if column in df.columns:
                valid_count = df[column].between(range_config['min'], range_config['max'], inclusive="both").sum()
                accuracy = valid_count / len(df) if len(df) > 0 else 1
                results[f'accuracy_{column}'] = {
                    'metric': 'accuracy',
                    'column': column,
                    'valid_count': valid_count,
                    'invalid_count': len(df) - valid_count,
                    'accuracy_rate': accuracy,
                    'threshold': rules['accuracy_threshold'],
                    'passed': accuracy >= rules['accuracy_threshold']
                }
        return results

    def run_all_checks(self, df, table_name):
        """Run semua quality checks"""
        checks = {}
        checks.update(self.check_completeness(df, table_name))
        checks.update(self.check_uniqueness(df, table_name))
        checks.update(self.check_accuracy(df, table_name))

        passed_checks = sum(1 for check in checks.values() if check['passed'])
        total_checks = len(checks)
        quality_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 100

        result = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'quality_score': quality_score,
            'checks': checks,
            'overall_status': 'PASS' if quality_score >= 95 else 'WARNING' if quality_score >= 80 else 'FAIL'
        }

        self.quality_results.append(result)
        return result
