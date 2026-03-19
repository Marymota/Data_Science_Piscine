import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt

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

def pie_chart(engine):
    table_customers = 'customers'
    try:
        with engine.begin() as conn:
            print(f"Creating temp table temp_{table_customers} ...")
            result = conn.execute(text(f"""
                SELECT
                    COUNT(*) FILTER (WHERE event_type = 'view') AS view,
                    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase,
                    COUNT(*) FILTER (WHERE event_type = 'remove_from_cart') AS remove_from_cart,
                    COUNT(*) FILTER (WHERE event_type = 'cart') AS cart
                FROM {table_customers};
            """))
            row = result.fetchone()
            view, purchase, remove_from_cart, cart = row
            print("Data retrieved successfully.")
    except Exception as e:
        print(f"Error retrieving data: {e}")
        return False

    labels = 'view', 'cart', 'remove_from_cart', 'purchase'
    sizes = [view, cart, remove_from_cart, purchase]
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    plt.show()
    return True



# Main
def main():
    # Database connection
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return 
    
    return pie_chart(engine)


if __name__ == "__main__":
    main()