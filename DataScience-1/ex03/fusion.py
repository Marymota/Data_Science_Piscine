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

        
def join_tables(table_customers, table_items, engine):

    try:
        with engine.begin() as conn:
            # Drop temp table if exists and create a new one with lag column
            print(f"Drop temp table if exists...")
            conn.execute(text(f"DROP TABLE IF EXISTS temp_{table_customers};"))
            print(f"Create a temp table with cool features...")
            conn.execute(text(f"""
                CREATE TABLE temp_{table_customers} AS
                SELECT * 
                FROM {table_customers}
                LEFT JOIN {table_items}
                USING (product_id);
            """))

            # Drop temp table afterwards
            print(f"Drop original {table_customers} and {table_items}...")
            conn.execute(text(f"DROP TABLE {table_customers};"))
            conn.execute(text(f"DROP TABLE {table_items};"))
            print(f"Rename temp_{table_customers}")
            conn.execute(text(f"ALTER TABLE temp_{table_customers} RENAME TO {table_customers};"))

        return True

    except Exception as e:
        print(f"Error joining tables as {table_customers}: {e}")
        return False


def main():
    # Database connection
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return 

    table_customers = "customers"
    table_items = "items"

    # Join Tables
    try:
        if join_tables(table_customers, table_items, engine):
            print(f"Joined {table_customers} and {table_items} successfully.")
        else:
            print("Failed to join tables.")

    except Exception as e:
        print(f"Error joining tables as {table_customers}: {e}")
        return False

# Run the script
if __name__ == "__main__":
    main()


#COMMANDS FOR TESTING
#   SELECT * FROM public.customers
#   WHERE product_id = 5802443 AND event_type = 'remove_from_cart'
#   ORDER BY event_time ASC;
#
#   SELECT COUNT(*) FROM customers;
#   19175899 -> 45899928