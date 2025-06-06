import os
import io
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO
from sqlalchemy import create_engine, text


#  Load environment variables from .env
load_dotenv()

# Connect to the PostgreSQL
def connect_to_database():
    host = 'localhost'  # <-- Removed the comma
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    try:
        conn = create_engine(f"postgresql://{user}:{password}@{host}/{database}")
        print("Connected to database")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


# # Get the correct datatype to use in SQL queries with pandas
# def map_dtype_to_sql(dtype):
#     print(f"Mapping datatypes to {table_name}")
#     if pd.api.types.is_integer_dtype(dtype):
#         return "INTEGER"
#     elif pd.api.types.is_float_dtype(dtype):
#         return "FLOAT"
#     elif pd.api.types.is_bool_dtype(dtype):
#         return "BOOLEAN"
#     elif pd.api.types.is_datetime64_dtype(dtype):
#         return "TIMESTAMP"
#     else:
#         return "TEXT"

def read_table_header(csv_path, table_name):
    try:
        # Instead of inferring types from CSV, just return fixed schema
        columns = [
            '"event_time" TIMESTAMP WITH TIME ZONE',
            '"event_type" VARCHAR(20)',
            '"product_id" INTEGER',
            '"price" DECIMAL(10,2)',
            '"user_id" BIGINT',
            '"user_session" UUID'
        ]
        print(f"Get columns for {table_name}")
        return columns  # Return the list of columns with types

    except Exception as e:
        print(f"Error getting columns from {table_name}: {e}")
        return None

# def create_table(csv_path, table_name, conn):
#     try:
#         # Read only the header row to get the name and # of columns
#         df = pd.read_csv(csv_path, nrows=1)

#         # for col in df.columns:
#         #     # Infer and give the right datatype to columns
#         #     sql_type = map_dtype_to_sql(df[col].dtype) 
#         #     columns.append(f'"{col}" {sql_type}')

#         sql = f"""
#         CREATE TABLE {table_name} (
#             event_time TIMESTAMP WITH TIME ZONE,
#             event_type VARCHAR(20),
#             product_id INTEGER,
#             price DECIMAL(10,2),
#             user_id BIGINT,
#             user_session UUID
#         );"""

#         cursor = conn.cursor()
#         cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
#         cursor.execute(sql)
#         conn.commit()
#         cursor.close()

        
#         print(f"Created table {table_name}")
#         return True
#     except Exception as e:
#         print(f"Error creating table {table_name}: {e}")
#         return False

# def load_table(engine, table_name, chunksize=10000):
#     print(f"Loading table {table_name}")
#     try:
#         query = f"SELECT * FROM {table_name}"
#         chunks = pd.read_sql(query, engine, chunksize=chunksize) # Avoids memory spikes
#         df = pd.concat(chunks, ignore_index=True)
#         return df
#     except Exception as e:
#         print(f"Failed to load table {table_name}: {e}")
#         return None

def remove_duplicates(table_name, engine):
    try:
        with engine.begin() as conn:
            # Drop temp table if exists and create a new one with lag column
            print(f"Drop temp table if exists...")
            conn.execute(text(f"DROP TABLE IF EXISTS temp_{table_name};"))
            print(f"Create a temp table with cool features...")
            conn.execute(text(f"""
                CREATE TABLE temp_{table_name} AS
                WITH marked AS (
                    SELECT *,
                           LAG(event_time) OVER (
                               PARTITION BY event_type, product_id, price, user_id, user_session
                               ORDER BY event_time
                           ) AS prev_time
                    FROM {table_name}
                ),
                filtered AS (
                    SELECT * FROM marked 
                    WHERE prev_time IS NULL 
                       OR EXTRACT(EPOCH FROM (event_time - prev_time)) > 1
                )
                SELECT event_time, event_type, product_id, price, user_id, user_session FROM filtered;
            """))

            # Drop temp table afterwards
            print(f"Drop original {table_name}...")
            conn.execute(text(f"DROP TABLE {table_name};"))
            print(f"Rename temp_{table_name}")
            conn.execute(text(f"ALTER TABLE temp_{table_name} RENAME TO {table_name};"))

        return True

    except Exception as e:
        print(f"Error removing duplicates from {table_name}: {e}")
        return False
    
# def update_table(engine, table_name, clean_table):

#     # Export DataFrame to CSV in memory
#     csv_buffer = io.StringIO()
#     clean_table.to_csv(csv_buffer, index=False, header=False)
#     csv_buffer.seek(0)

#     try:
#         raw_conn = engine.raw_connection()

#         # Clear existing data
#         cursor = raw_conn.cursor()
#         print(f"Truncating table {table_name}")
#         cursor.execute(f'TRUNCATE TABLE "{table_name}";')

#         # Insert new data
#         columns = ','.join(clean_table.columns)
#         print(f"Inserting data into {table_name} using COPY")
#         print(f"Cleaning table {table_name}")
#         cursor.copy_expert(f'COPY "{table_name}" ({columns}) FROM STDIN WITH CSV', csv_buffer)
#         raw_conn.commit()
#     except Exception as e:
#         print(f"Error updating table '{table_name}': {e}")
#     finally:
#         raw_conn.close()
#     print("Table updated successfully.")



def main():
    # Database connection
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return 

    table_name = "customers"
   
    # Remove duplicates
    if remove_duplicates(table_name, engine):
        print(f"Duplicates removed from {table_name} successfully.")
        
        # After removing duplicates I was loading the table back into a DataFrame with load_table(), only to rewrite the same data back into the same table using update_table().
        # # Reload cleaned table and update it
        # clean_table = load_table(engine, table_name)
        # if clean_table is not None:
        #     update_table(engine, table_name, clean_table)
        # else:
            # print("Failed to reload cleaned table.")
    else:
        print("Failed to remove duplicates.")


# Run the script
if __name__ == "__main__":
    main()


#COMMANDS FOR TESTING
#   SELECT * FROM public.customers
#   WHERE product_id = 5802443 AND event_type = 'remove_from_cart'
#   ORDER BY event_time ASC;
#
#   SELECT COUNT(*) FROM customers;