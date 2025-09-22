from scripts.extract import extract_from_folder
#from scripts.transform import clean_retail_data
#from scripts.load import load_to_sql

def main():
    # Extract
    df = extract_from_folder("../data/raw/rakamin", recursive=True)

    # Transform
    #df_clean = clean_retail_data(df)

    # Load
    #load_to_sql(df_clean, table_name="retail_sales")

if __name__ == "__main__":
    main()
