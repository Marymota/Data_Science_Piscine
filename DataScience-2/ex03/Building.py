import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Load environment variables from .env
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

def building(engine):
    try:
        # Frequency of purchases for each customer
        query_customers = """
            SELECT user_id, COUNT(*) AS frequency
            FROM customers
            WHERE event_type = 'purchase'
            AND price > 0
            GROUP BY user_id
        """
        df_frequency = pd.read_sql(query_customers, engine)

        # Monetary value of purchases for each customer
        query_monetary = """
            SELECT user_id, SUM(price) AS monetary
            FROM customers
            WHERE event_type = 'purchase'
            AND price > 0
            AND price IS NOT NULL
            GROUP BY user_id;
        """

        # --- PLOT 1: bar chart with the number of orders according to the frequency ---
        fig, ax1 = plt.subplots(figsize=(7, 5))
        ax1.set_facecolor("#edf1f7")
        ax1.grid(axis="y", color="#d8dfe8", linewidth=0.8, zorder=0)
        ax1.set_axisbelow(True)
        for spine in ax1.spines.values():
            spine.set_visible(False)
        ax1.tick_params(length=0)

        bins1 = ([0, 10, 20, 30, 40, 50])
        counts1, edges1 = np.histogram(df_frequency['frequency'], bins=list(bins1))
        ax1.bar(edges1[:-1], counts1, width=np.diff(edges1) * 0.9,
                align='edge', color='#a8bcd4', zorder=2)
        ax1.set_xlabel("frequency")
        ax1.set_xticks([0, 10, 20, 30, 40])
        ax1.set_xticklabels([0, 10, 20, 30, 40])
        ax1.set_ylabel("customers")
        ax1.set_yticks([0,10000, 20000, 30000, 40000, 50000, 60000])
        ax1.set_xlim(-2, 50)
        plt.tight_layout()
        plt.show()

        df_monetary = pd.read_sql(query_monetary, engine)
        df_monetary = df_monetary[df_monetary['monetary'] > 0]
        
        # --- PLOT 2 ---
        fig, ax2 = plt.subplots(figsize=(7, 5))
        ax2.set_facecolor("#edf1f7")
        ax2.grid(axis="y", color="#d8dfe8", linewidth=0.8, zorder=0)
        ax2.set_axisbelow(True)
        for spine in ax2.spines.values():
            spine.set_visible(False)
        ax2.tick_params(length=0)
        bins2 = [0, 50, 100, 150, 200, 250]
        counts2, edges2 = np.histogram(df_monetary['monetary'], bins=bins2)
        ax2.bar(edges2[:-1], counts2, width=np.diff(edges2) * 0.9,
                align='edge', color='#a8bcd4', zorder=2)
        ax2.set_xlabel("monetary value in ₳")
        ax2.set_ylabel("customers")
        ax2.set_xticks([25, 75, 125, 175, 225])
        ax2.set_xticklabels([0, 50, 100, 150, 200])
        ax2.set_xlim(-10, 250)
        ax2.set_yticks([0, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000])
        ax2.set_ylim(0, 45000)
        plt.tight_layout()
        plt.show()

        plt.clf()
        plt.close('all')

    except Exception as e:
        print(f"Error retrieving data: {e}")
        return False
    return True

def main():
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return
    return building(engine)

if __name__ == "__main__":
    main()