import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np


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
    try:
        print(f"Creating temp table temp_{table_customers} ...")
        query_customers = """
            SELECT
                DATE_TRUNC('day', event_time) AS date,
                COUNT(DISTINCT user_id) AS customers
            FROM customers
            WHERE event_type = 'purchase'
            GROUP BY date
            ORDER BY date
        """
         # Deduplicate purchases by user_id and event_time to avoid counting multiple purchases by the same user in the same day
         # with "SELECT DISTINCT event_time, price, user_id"
         # Then, we sum the prices and divide by 1 million to convert to millions of ₳
        query_sales = """
            SELECT
                DATE_TRUNC('month', event_time) AS date,
                SUM(CAST(price AS FLOAT)) / 1000000 AS total_sales
            FROM (
                SELECT DISTINCT event_time, price, user_id
                FROM customers
                WHERE event_type = 'purchase'
                AND price > 0
            ) AS deduped
            GROUP BY date
            ORDER BY date
        """
        query_spending = """
            SELECT
                DATE_TRUNC('day', event_time) AS date,
                SUM(price) / COUNT(DISTINCT user_id) AS avg_spend
            FROM (
                    SELECT DISTINCT event_time, price, user_id
                    FROM customers
                    WHERE event_type = 'purchase'
                    AND price > 0
                ) AS deduped
            GROUP BY date
            ORDER BY date
        """
        print("Data retrieved successfully.")
        # Charts customization
        sns.set_theme(style="darkgrid")
        # Chart customers - Number of customers
        df = pd.read_sql(query_customers, engine)
        df["date"] = pd.to_datetime(df["date"])
        fig, ax = plt.subplots()
        sns.lineplot(data=df, x="date", y="customers", ax=ax)
        ax.set_ylabel("Number of costumers")
        ax.set_xlabel("")
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.margins(x=0)
        plt.tight_layout()
        plt.show()

        # Chart sales - Total sales
        df = pd.read_sql(query_sales, engine)
        df["date"] = pd.to_datetime(df["date"])
        fig, ax = plt.subplots()
        sns.barplot(data=df, x="date", y="total_sales", ax=ax, color="#bcd4e6")
        ax.set_ylabel("total sales in millions of ₳") # ₳ is the symbol for the currency of Atlantis
        ax.set_ylim(bottom=0)
        ax.set_xlabel("month")
        ax.set_xticklabels([d.strftime("%b") for d in df["date"]])
        ax.set_xticks(range(len(df["date"])))
        ax.set_yticks(np.arange(0, 1.4, 0.2))
        plt.tight_layout()
        plt.show()

        # Chart spending - Average spending per customer
        df = pd.read_sql(query_spending, engine)
        df["date"] = pd.to_datetime(df["date"])
        fig, ax = plt.subplots()
        ax.fill_between(df["date"], df["avg_spend"], color="#bcd4e6")
        ax.set_ylabel("average spend/customers in ₳")
        ax.set_yticks(range(0, 45, 5))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.margins(x=0)
        ax.set_ylim(bottom=0)
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Error retrieving data: {e}")
        return False
    return True
    

def main():
    # Database connection
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return
    
    return charts(engine)

if __name__ == "__main__":
    main()