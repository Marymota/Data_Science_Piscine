import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import pandas as pd

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
    query = """
        SELECT event_type, COUNT(*) AS total
        FROM customers
        GROUP BY event_type
        ORDER BY total DESC
    """
    df = pd.read_sql(query, engine)

    fig, ax = plt.subplots()
    ax.pie(
        df["total"],
        labels=df["event_type"],
        autopct="%1.1f%%",
        explode= [0.005] * len(df)
    )
    plt.tight_layout() 
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