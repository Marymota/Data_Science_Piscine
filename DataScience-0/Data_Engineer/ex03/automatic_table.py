import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
conn = psycopg2.connect(database = os.getenv("POSTGRES_DB"),
                        user = os.getenv("POSTGRES_USER"), 
                        password = os.getenv("POSTGRES_PASSWORD"), 
                        host='postgres', port='5432')

conn.autocommit = True
cursor = conn.cursor()


sql = '''CREATE TABLE data_2022_oct (
event_time TIMESTAMP WITH TIME ZONE,
event_type VARCHAR(50),
product_id INTEGER, 
price DECIMAL(10,2),
user_id BIGINT,
user_session UUID);'''


cursor.execute(sql)

conn.commit()
conn.close()