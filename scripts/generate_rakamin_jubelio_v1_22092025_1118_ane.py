# scripts/generate_synthetic_orders.py
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# Config
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
DB_PATH = os.path.join(BASE_DIR, "data", "database", "my_database.db")

N_FILES = 2           # number of raw files to generate
ROWS_PER_FILE = 20    # rows per file

np.random.seed(42)

def generate_orders(n_rows: int):
    """Generate synthetic order data with shipping labels."""
    order_ids = np.arange(1, n_rows + 1)
    customer_ids = np.random.randint(1000, 2000, size=n_rows)
    items = np.random.choice(["Laptop", "Phone", "Tablet", "Headphones", "Camera"], size=n_rows)
    quantities = np.random.randint(1, 5, size=n_rows)
    prices = np.round(np.random.uniform(50, 2000, size=n_rows), 2)
    total = quantities * prices
    labels = np.random.choice(["STANDARD", "EXPRESS", "ECONOMY", "PICKUP"], size=n_rows)

    df = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "item": items,
        "quantity": quantities,
        "price": prices,
        "total": total,
        "shipping_label": labels,
        "order_date": pd.to_datetime("today").normalize()
    })
    return df

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    today_str = datetime.today().strftime("%Y%m%d")

    # Generate multiple raw files
    raw_files = []
    for i in range(1, N_FILES + 1):
        df = generate_orders(ROWS_PER_FILE)
        file_path = os.path.join(RAW_DIR, f"orders_{today_str}_v{i}.csv")
        df.to_csv(file_path, index=False)
        raw_files.append(file_path)
    print(f"Generated raw CSVs: {raw_files}")

    # Combine all raw files into one DataFrame
    combined_df = pd.concat([pd.read_csv(f) for f in raw_files], ignore_index=True)

    # Save combined data to SQLite
    conn = sqlite3.connect(DB_PATH)
    combined_df.to_sql("orders", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Inserted data into database: {DB_PATH}")

    # Export processed orders
    export_orders_path = os.path.join(PROCESSED_DIR, f"orders_export_{today_str}.csv")
    combined_df.to_csv(export_orders_path, index=False)
    print(f"Exported combined orders: {export_orders_path}")

    # Export summary of shipping labels
    summary = combined_df.groupby("shipping_label").size().reset_index(name="count")
    export_labels_path = os.path.join(PROCESSED_DIR, f"shipping_labels_summary_{today_str}.csv")
    summary.to_csv(export_labels_path, index=False)
    print(f"Exported shipping labels summary: {export_labels_path}")

if __name__ == "__main__":
    main()
