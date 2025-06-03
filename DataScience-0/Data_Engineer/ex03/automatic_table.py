#   docker exec -i postgres psql -U your_login -d piscineds < automatic_table.py

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

def get_csv_files():
    customer = 'customer/customer'
    csv_files = []
    
    for file in os.listdir(customer):
        if file.endswith('.csv'):
            csv_files.append(file)    
    
    print(f"Found CSV files: {csv_files}")
    return csv_files

def create_table(table_name, conn):
    sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        event_time TIMESTAMP WITH TIME ZONE,
        event_type VARCHAR(50),
        product_id INTEGER,
        price DECIMAL(10,2),
        user_id BIGINT,
        user_session UUID
    );"""
    
    try:
        cursor = conn.cursor() 
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        print(f"Table {table_name} created")
        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        return False


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

    # Create table for each CSV file
    for csv_file in csv_files:
        # Get table name (remove .csv extension)
        table_name = csv_file.replace('.csv', '')
        
        if create_table(table_name, conn):
            print(f"Table {table_name} created successfully.")
        else:   
            print(f"Failed to create table {table_name}.") 

    conn.close()

# Run the script
if __name__ == "__main__":
    main()