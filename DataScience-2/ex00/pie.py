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

def create_pie_chart(engine):
    try:
        # Read event_type counts from the customers table
        df = pd.read_sql_query(text("""
            SELECT
                event_type,
                COUNT(*) AS value
            FROM customers
            GROUP BY event_type
            ORDER BY
                CASE event_type
                    WHEN 'cart' THEN 1
                    WHEN 'remove_from_cart' THEN 2
                    WHEN 'purchase' THEN 3
                    WHEN 'view' THEN 4
                    ELSE 5
                END
        """), engine)
        plt.figure(figsize=(20, 20))
        colors = ['#dd8452', '#55a868', '#c44e52','#4c72b0',] 
        plt.pie(df['value'], labels=df['event_type'], autopct='%1.1f%%', startangle=180, colors=colors)
        plt.title('Customers Pie Chart')
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
    
    create_pie_chart(engine)


if __name__ == "__main__":
    main()