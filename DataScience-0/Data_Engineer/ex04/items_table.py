#   docker exec -i postgres psql -U your_login -d piscineds < items_table.py

import os
from dotenv import load_dotenv
import psycopg2
# Load environment variables from .env file 
load_dotenv()

# Connect to the PostgreSQL
def connect_to_database():
    try:
        conn = psycopg2.connect(
            host='postgres', 
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"), 
            password=os.getenv("POSTGRES_PASSWORD"), 
        )
        print("Connected to database")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_items_table(conn):
    sql = """
    DROP TABLE IF EXISTS items;
    CREATE TABLE items (
        product_id BIGINT,
        category_id INTEGER,
        category_code VARCHAR(255),
        brand VARCHAR(255)
    );"""
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        print("Items table created successfully")
        return True
    except Exception as e:
        print(f"Error creating items table: {e}")
        return False

def import_csv_data(conn):
    csv_file_path = '/app/costumer/item/item.csv'  # Full path in Docker container
    
    try:
        cursor = conn.cursor()
        with open('./costumer/item/item.csv', 'r') as f:
            cursor.copy_expert(
                "COPY items(product_id, category_id, category_code, brand) FROM STDIN WITH CSV HEADER",
                f
            )
        conn.commit()
        cursor.close()
        print("CSV data imported successfully")
        return True
    except Exception as e:
        print(f"Error importing CSV data: {e}")
        return False

def main():
    conn = connect_to_database()
    if not conn:
        print("Failed to connect to the database.")
        return
    
    # Create items table (Exercise 4 requirement)
    if create_items_table(conn):
        # Import CSV data
        if import_csv_data(conn):
            print("CSV data imported")
        else:
            print("Failed to import CSV data")
    else:
        print("Failed to create items table")
    
    conn.close()

if __name__ == "__main__":
    main()