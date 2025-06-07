import os
import io
from io import StringIO
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt


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

def create_pie(conn):
    # Read data from SQL table
    try:
        # Step 1: Create temp table
        with engine.begin() as conn:
            print(f"Creating the pie ...")
            conn.execute(text(f"""
                SELECT 
                    event_time, 
                    event_type, 
                    product_id, 
                    price, 
                    user_id,
                    session, 
                    category_id,
                    category_code,
                    brand
                FROM customers 
            """))

        # Create pie chart
        plt.figure(figsize=(8, 6))
        plt.pie(df['value'], labels=df['category'], autopct='%1.1f%%', startangle=90)
        plt.title('Data Distribution')
        plt.axis('equal')
        plt.show()
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

# Main
def main():
    # Database connection
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return 


if __name__ == "__main__":
    main()