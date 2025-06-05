import os
from dotenv import load_dotenv
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

# Get CSV files
def get_csv_files(customer):
    csv_files = []
    
    for file in os.listdir(customer):
        if file.endswith('.csv'):
            csv_files.append(file)    
    
    print(f"Found CSV files: {csv_files}")
    return csv_files

# Get the correct datatype to use in SQL queries with pandas
def map_dtype_to_sql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

def create_table(csv_path, table_name, conn):
    try:
        # Read only the header row to get the name and # of columns
        df = pd.read_csv(csv_path, nrows=1)
        columns = []

        for col in df.columns:
            # Infer and give the right datatype to columns
            sql_type = map_dtype_to_sql(df[col].dtype) 
            columns.append(f'"{col}" {sql_type}')
        
        query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns)});' 

        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
        
        print(f"Created table {table_name} with inferred types")
        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return False

def fill_table(csv_path, table_list, conn):
    cursor = conn.cursor()
    with open(csv_path, 'r') as f:
        try:
            cursor.copy_expert(sql=f"COPY {table_list} FROM STDIN WITH CSV HEADER", file=f)
            conn.commit()
            print(f"Table {table_list} filled")
        except Exception as e:
            conn.rollback()
            print(f"Error filling table {table_list}: {e}")
    cursor.close()

def combine_tables(conn, table_list, final_table_name):
    try: 
        with conn.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{final_table_name}";')
            # Unions all tables (assume same structure)
            union_query = f"""
                CREATE TABLE "{final_table_name}" AS
                {" UNION ALL ".join([f'SELECT * FROM "{table}"' for table in table_list])};
            """
            cursor.execute(union_query)
            conn.commit()
        print(f"Combined all the tables into {final_table_name})")
    except Exception as e:
        print(f"Error creating combined table: {e}")

def main():
    # Database connection
    conn = connect_to_database()
    if not conn:
        print("Failed to connect to the database.")
        return 

    # variables
    customer = "../ex00/subject/customer"
    final_table_name = "customers"

    # Get CSV files
    csv_files = get_csv_files(customer)
    if not csv_files:
        print("No CSV files found!")
        conn.close()
        return
    
    table_list = []
    # Create table for each CSV file
    for csv_file in csv_files:
        csv_path = os.path.join(customer, csv_file)
        table_name = csv_file.replace('.csv', '')

        if create_table(csv_path, table_name, conn):
            try:
                fill_table(csv_path, table_name, conn)
                table_list.append(table_name)
            except Exception as e:
                print(f"Error processing file {csv_file}: {e}")
                conn.rollback()
        else:
            print("Customer folder not found")

    if table_list:
        try:
            combine_tables(conn, table_list, final_table_name)
        except Exception as e:
            print(f"Error combining tables {csv_file}: {e}")

    conn.close()

# Run the script
if __name__ == "__main__":
    main()
