import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

# Connect to PostgreSQL
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

# Get a list of existing tables matching 'data_20%'
def get_existing_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' AND tablename LIKE 'data_20%';
            """)
            tables = [row[0] for row in cursor.fetchall()]
        print(f"Found tables: {tables}")
        return tables
    except Exception as e:
        print(f"Error retrieving tables: {e}")
        return []

# Combine tables using UNION ALL
def combine_tables(conn, table_list, final_table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{final_table_name}";')
            union_query = f"""
                CREATE TABLE "{final_table_name}" AS
                {" UNION ALL ".join([f'SELECT * FROM "{table}"' for table in table_list])};
            """
            cursor.execute(union_query)
            conn.commit()
        print(f"Combined all tables into '{final_table_name}'")
    except Exception as e:
        print(f"Error combining tables: {e}")

# Main
def main():
    conn = connect_to_database()
    if not conn:
        return

    final_table_name = "customers"
    table_list = get_existing_tables(conn)

    if not table_list:
        print("No tables found with prefix 'data_20'")
    else:
        combine_tables(conn, table_list, final_table_name)

    conn.close()

if __name__ == "__main__":
    main()