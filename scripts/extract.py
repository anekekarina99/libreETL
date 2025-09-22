# scripts/extract.py
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def read_csv_file(file_path):
    """Read CSV file into a DataFrame"""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read CSV: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV {file_path}: {str(e)}")
        raise

def read_excel_file(file_path, sheet_name=0):
    """Read Excel file into a DataFrame"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        logger.info(f"Successfully read Excel: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel {file_path}: {str(e)}")
        raise

def read_json_file(file_path):
    """Read JSON file into a DataFrame"""
    try:
        df = pd.read_json(file_path)
        logger.info(f"Successfully read JSON: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read JSON {file_path}: {str(e)}")
        raise

def read_parquet_file(file_path):
    """Read Parquet file into a DataFrame"""
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Successfully read Parquet: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Parquet {file_path}: {str(e)}")
        raise

def extract_data(file_path):
    """
    Main function to extract data from a single file,
    supporting multiple formats.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == '.csv':
        return read_csv_file(file_path)
    elif suffix in ['.xlsx', '.xls']:
        return read_excel_file(file_path)
    elif suffix == '.json':
        return read_json_file(file_path)
    elif suffix == '.parquet':
        return read_parquet_file(file_path)
    else:
        logger.error(f"Unsupported file format: {suffix}")
        raise ValueError(f"Unsupported file format: {suffix}")

def extract_from_folder(folder_path, recursive=True):
    """
    Extract all supported files within a folder (recursively if True).
    Returns a single concatenated DataFrame.
    """
    folder_path = Path(folder_path)
    if not folder_path.exists() or not folder_path.is_dir():
        raise NotADirectoryError(f"Not a valid folder: {folder_path}")

    all_dfs = []
    patterns = ["*.csv", "*.xlsx", "*.xls", "*.json", "*.parquet"]

    for pattern in patterns:
        files = folder_path.rglob(pattern) if recursive else folder_path.glob(pattern)
        for f in files:
            try:
                df = extract_data(f)
                df["source_file"] = f.name  # add a column to track origin
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"Skipping file {f}: {e}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        logger.warning("No files were successfully read.")
        return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    try:
        # Single file
        df_single = extract_data("../data/raw/sample_file.csv")
        print("Single file shape:", df_single.shape)

        # Whole folder
        df_folder = extract_from_folder("../data/raw/", recursive=True)
        print("Combined folder shape:", df_folder.shape)
        print(df_folder.head())
    except Exception as e:
        print(f"Error: {str(e)}")
