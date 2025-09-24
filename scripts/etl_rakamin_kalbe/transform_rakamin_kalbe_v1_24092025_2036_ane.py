import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
from pathlib import Path

# Base path configuration
BASE_DIR = Path(__file__).parent.parent.parent

def clean_customer_data(df_customers):
    """
    Transformasi data customer
    """
    # Handle missing values
    df_clean = df_customers.copy()
    
    # Standardize text columns
    if 'customer_name' in df_clean.columns:
        df_clean['customer_name'] = df_clean['customer_name'].str.title().str.strip()
    
    # Handle missing values
    df_clean.fillna({
        'email': 'unknown@email.com',
        'phone': '0000000000'
    }, inplace=True)
    
    # Add timestamp
    df_clean['processed_at'] = datetime.now()
    
    return df_clean

def transform_orders(df_orders, df_customers):
    """
    Transformasi data orders dengan join customer
    """
    # Merge dengan customer data
    df_transformed = pd.merge(
        df_orders, 
        df_customers[['customer_id', 'customer_name', 'segment']], 
        on='customer_id', 
        how='left'
    )
    
    # Convert date columns
    date_columns = ['order_date', 'ship_date']
    for col in date_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
    
    # Calculate derived metrics
    if all(col in df_transformed.columns for col in ['quantity', 'unit_price']):
        df_transformed['total_amount'] = df_transformed['quantity'] * df_transformed['unit_price']
    
    # Filter valid records
    df_transformed = df_transformed[df_transformed['order_date'].notna()]
    
    logging.info(f"Orders transformed: {len(df_transformed)} records")
    return df_transformed

def create_sales_summary(df_orders):
    """
    Buat summary sales aggregasi
    """
    summary = df_orders.groupby([
        pd.Grouper(key='order_date', freq='D'),  # Group by day
        'customer_segment'
    ]).agg({
        'total_amount': ['sum', 'mean', 'count'],
        'quantity': 'sum'
    }).round(2)
    
    summary.columns = ['total_sales', 'avg_order_value', 'order_count', 'total_quantity']
    summary = summary.reset_index()
    
    return summary