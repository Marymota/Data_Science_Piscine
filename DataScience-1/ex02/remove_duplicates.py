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
    host = 'localhost'
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

def remove_duplicates(table_name, engine):
    try:
        with engine.begin() as conn:
            # Drop temp table if exists and create a new one with lag column
            print(f"Drop temp table if exists...")
            conn.execute(text(f"DROP TABLE IF EXISTS temp_{table_name};"))
            print(f"Create a temp table with cool features...")
            # Table cleaning logic:
            # DISTINCT * : remove fully identical rows
            # LAG() → filter gap > 1s
            # DISTINCT ON (everything the same but the session)
            conn.execute(text(f"""
            CREATE TABLE temp_customers AS
            WITH dup AS (
                SELECT DISTINCT *  
                FROM customers
            ),
            time_filtered AS (
                SELECT *,
                    LAG(event_time) OVER (
                        PARTITION BY user_id, user_session, event_type, product_id
                        ORDER BY event_time
                    ) AS prev_time
                FROM dup
            )
            SELECT event_time, event_type, product_id, price, user_id, user_session
            FROM time_filtered
            WHERE prev_time IS NULL
            OR EXTRACT(EPOCH FROM (event_time - prev_time)) > 1;
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