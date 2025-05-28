import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="retail",
    user="postgres",
    password="Jotsna@123",
    port="5432"
)

query = "SELECT * FROM transactions LIMIT 10;"
df = pd.read_sql_query(query, conn)
conn.close()

print("\n🧪 Sample data:")
print(df)

print("\n🔍 Data types:")
print(df.dtypes)
