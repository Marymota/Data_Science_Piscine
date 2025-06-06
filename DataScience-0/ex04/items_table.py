import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import csv
from datetime import datetime

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
def get_csv_files():
    item = 'item'
    csv_files = []
    
    for file in os.listdir(item):
        if file.endswith('.csv'):
            csv_files.append(file)    
    
    print(f"Found CSV files: {csv_files}")
    return csv_files

def create_table(csv_path, table_name, conn):
    with open(csv_path, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)

    cursor = conn.cursor()

    sql = f"""
    CREATE TABLE {table_name} (
        product_id INTEGER CHECK (product_id >= 0 AND product_id <= 9999999),
        category_id NUMERIC(20,0),
        category_code VARCHAR(100),
        brand VARCHAR(20)
    );"""

    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor = conn.cursor() 
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        print(f"Table {table_name} created")
        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return False

# def fill_table(csv_path, table_name, conn):
#     cursor = conn.cursor()

#     with open(csv_path, 'r') as file:
#         reader = csv.reader(file)
#         headers = next(reader)
#         print(f"Headers in CSV: {headers}, length: {len(headers)}")

#         placeholders = ', '.join(['%s'] * len(headers))
#         sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

#         for row in reader:
#             if row[-1] == '':
#                 row[-1] = None
#             rows.append(row)
#         cursor.executemany(sql, rows)

#     conn.commit()
#     cursor.close()
#     print(f"Table {table_name} filled")

def fill_table(csv_path, table_name, conn):
    cursor = conn.cursor()
    with open(csv_path, 'r') as f:
        try:
            cursor.copy_expert(sql=f"COPY {table_name} FROM STDIN WITH CSV HEADER", file=f)
            conn.commit()
            print(f"Table {table_name} filled")
        except Exception as e:
            conn.rollback()
            print(f"Error filling table {table_name}: {e}")
    cursor.close()

def main():
    conn = connect_to_database()
    if not conn:
        print("Failed to connect to the database.")
        return    

    # Get CSV files
    csv_files = get_csv_files()
    if not csv_files:
        print("No CSV files found!")
        conn.close()
        return
    
    item = "./item"

    # Create table for each CSV file
    for csv_file in csv_files:
        csv_path = os.path.join(item, csv_file)
        # table_name = csv_file.replace('.csv', '')
        table_name = "items"
        
        if create_table(csv_path, table_name, conn):
            try:
                fill_table(csv_path, table_name, conn)
            except Exception as e:
                print(f"Error processing file {csv_file}: {e}")
        else:
            print("items folder not found")

    conn.close()

# Run the script
if __name__ == "__main__":
    main()
