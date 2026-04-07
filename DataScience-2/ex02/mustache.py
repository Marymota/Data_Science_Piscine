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
    
def mustaches(engine):
    try:
        query_customers = """
            SELECT price, user_id, session
            FROM customers
            WHERE event_type = 'purchase'
        """
        df = pd.read_sql(query_customers, engine)
        
        # Metrics Calculation
        count = df['price'].count()
        mean = df['price'].mean()
        std = df['price'].std()
        min = df['price'].min()
        q1 = df['price'].quantile(0.25)
        q2 = df['price'].median()
        q3 = df['price'].quantile(0.75)
        max = df['price'].max()

        print(f"count {count:.6f}")
        print(f"mean {mean:.6f}")
        print(f"std {std:.6f}")
        print(f"min {min:.6f}")
        print(f"25% {q1:.6f}")
        print(f"50% {q2:.6f}")
        print(f"75% {q3:.6f}")
        print(f"max {max:.6f}")

        # --- PLOT 1:  box plots that display the price of the items purchased
        plt.figure(figsize=(5, 4))
        sns.set_style("darkgrid")
        sns.boxplot(
            x=df['price'], 
            color='#5c7a67', 
            fliersize=4,
            flierprops=dict(
                marker='D', 
                markerfacecolor='#5c7a67', 
                markeredgecolor='#5c7a67'
            )
        )
        plt.xlabel('price')
        plt.tight_layout()
        plt.show()

        # --- PLOT 2:  box plots that display the price of the items purchased without outliers
        plt.figure(figsize=(5, 4))
        sns.set_style("darkgrid")
        ax = sns.boxplot(
            x=df['price'], 
            color='#7eb571', 
            showfliers=False
            )
        ax.set_xlim(-1, 13)
        plt.xlabel('price')
        plt.tight_layout()
        plt.show()  
        plt.clf()
        plt.close('all')
        

# --- PLOT 3: Then a box plot with the average basket price per user ---
        total_price_per_user = df.groupby('user_id')['price'].sum().reset_index(name='total_price')
        total_transactions_per_user = df.groupby('user_id')['session'].nunique().reset_index(name='total_transactions')
        avgBasketPricePerUser = total_price_per_user.merge(total_transactions_per_user, on='user_id')
        avgBasketPricePerUser['basket_avrg'] = avgBasketPricePerUser['total_price'] / avgBasketPricePerUser['total_transactions']
        
        fig, ax = plt.subplots(figsize=(5, 4))
        sns.set_style("darkgrid")
        sns.boxplot(
            x=avgBasketPricePerUser['basket_avrg'],
            color='#4a90e2',
            fliersize=4,
            flierprops=dict(marker='D', markerfacecolor='#4a90e2', markeredgecolor='#4a90e2'),
            showfliers=True,
            ax=ax
        )
        ax.set_xlim(-1, 100)
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
    
    return mustaches(engine)

if __name__ == "__main__":
    main()