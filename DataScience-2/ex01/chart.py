import os
from dotenv import load_dotenv
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
    
def charts(engine):
    table_customers = 'customers'
    # try:
    #     with engine.begin() as conn:
    #         print(f"Creating temp table temp_{table_customers} ...")
    #         result = conn.execute(text(f"""
    #             SELECT
    #                 SUM(CASE WHEN event_type = 'purchase')
    #             FROM {table_customers};
    #         """))        

def main():
    # Database connection
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return
    
    return charts(engine)

if __name__ == "__main__":
    main()