import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

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
    
def elbow(engine):
    try:
        query = """
            SELECT user_id, 
                   COUNT(*) AS frequency, 
                   SUM(price) AS monetary
            FROM customers
            WHERE event_type = 'purchase' AND price > 0 
            GROUP BY user_id
        """
        df = pd.read_sql(query, engine)

        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(df[['frequency', 'monetary']])

        inertia = []
        for k in range(1, 7):
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(scaled_features)
            inertia.append(kmeans.inertia_)

        fig, ax = plt.subplots(figsize=(7, 6))
        ax.set_facecolor("#edf1f7")
        # Keep grid zorder at 0 to stay behind the line
        ax.grid(color="#ffffff", linewidth=0.8, zorder=0)
        
        ax.plot(range(1, 7), inertia, color='#4c72b0', linewidth=1.5, zorder=3)

        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.set_title("The Elbow Method", fontsize=12, pad=20, color="#575555")
        ax.set_xlabel("Number of clusters", fontsize=10)
        
        ax.tick_params(axis='both', which='major', labelsize=10, length=0)
        
        plt.xticks([2, 4, 6])
        plt.yticks([0, 50000, 100000, 150000, 200000, 250000])
        plt.ylim(0, 250000)

        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"Error: {e}")

def main():
    engine = connect_to_database()
    if not engine:
        print("Failed to connect to the database.")
        return
    return elbow(engine)

if __name__ == "__main__":
    main()