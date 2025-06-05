import os
from dotenv import load_dotenv
import glob
import pandas as pd
import psycopg2

#  Load environment variables from .env
load_dotenv()

# Connect to the PostgreSQL
def connect_to_database():
    try:
        conn = psycopg2.connect(
            host='localhost', 
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"), 
            password=os.getenv("POSTGRES_PASSWORD"), 
        )
        print("Connected to database")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Connect to the PostgreSQL
def get_csv_files(customer):
    csv_files = []
    
    for file in os.listdir(customer):
        if file.endswith('.csv'):
            csv_files.append(file)    
    
    print(f"Found CSV files: {csv_files}")
    return csv_files

def join_tables(customer, all_filenames):
    print("Initiating tables join")
    print(f"Files to be combined: {all_filenames}")

    try:
        # Construct full paths to each file
        full_paths = [os.path.join(customer, f) for f in all_filenames]

        #combine all files in the list
        combined_csv = pd.concat([pd.read_csv(f) for f in full_paths ])
        print("Combined all files in the list")

        #export to csv
        combined_csv.to_csv( "customers_table.csv", index=False, encoding='utf-8-sig')
        print("export to csv")
    except Exception as e:
        print(f"Join tables failed {e}")

def main():
    conn = connect_to_database()
    if not conn:
        print("Failed to connect to the database.")
        return 

    customer = "../../subject/customer"

    # Get CSV files
    csv_files = get_csv_files(customer)
    if not csv_files:
        print("No CSV files found!")
        conn.close()
        return
    

    # Create table for each CSV file
    for csv_file in csv_files:
        csv_path = os.path.join(customer, csv_file)
        table_name = csv_file.replace('.csv', '')
        
        # if create_table(csv_path, table_name, conn):
        #     try:
        #         fill_table(csv_path, table_name, conn)
        #     except Exception as e:
        #         print(f"Error processing file {csv_file}: {e}")
        # else:
        #     print("Customer folder not found")

    join_tables(customer, csv_files)

    conn.close()

# Run the script
if __name__ == "__main__":
    main()
