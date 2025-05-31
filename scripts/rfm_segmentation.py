import psycopg2
import pandas as pd
from sklearn.cluster import KMeans
from datetime import datetime
import os

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="retail",
    user="postgres",
    password="Jotsna@123",  # Replace with your actual password
    port="5432"
)

# Load data
query = "SELECT customer_id, price, timestamp FROM transactions WHERE timestamp IS NOT NULL;"
df = pd.read_sql_query(query, conn)
conn.close()

# Convert timestamp to datetime and clean price
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
df['price'] = pd.to_numeric(df['price'], errors='coerce')  # Ensure price is float
df.dropna(subset=['price', 'timestamp'], inplace=True)     # Drop any invalid rows

# Today's date
today = datetime.today()

# Recency in days
recency_df = df.groupby('customer_id')['timestamp'].max().reset_index()
recency_df['Recency'] = recency_df['timestamp'].apply(lambda x: (today - x).days)
recency_df.drop(columns=['timestamp'], inplace=True)

# Frequency
frequency_df = df.groupby('customer_id').size().reset_index(name='Frequency')

# Monetary
monetary_df = df.groupby('customer_id')['price'].sum().reset_index()
monetary_df.rename(columns={'price': 'Monetary'}, inplace=True)

# Merge into one RFM DataFrame
rfm = recency_df.merge(frequency_df, on='customer_id').merge(monetary_df, on='customer_id')

# Coerce types for clustering
rfm['Recency'] = pd.to_numeric(rfm['Recency'], errors='coerce')
rfm['Frequency'] = pd.to_numeric(rfm['Frequency'], errors='coerce')
rfm['Monetary'] = pd.to_numeric(rfm['Monetary'], errors='coerce')

# Drop any invalid rows before clustering
rfm.dropna(subset=['Recency', 'Frequency', 'Monetary'], inplace=True)

# DEBUG
print("Cleaned RFM dtypes:\n", rfm[['Recency', 'Frequency', 'Monetary']].dtypes)
print("Sample RFM rows:\n", rfm[['Recency', 'Frequency', 'Monetary']].head())

# Clustering
X = rfm[['Recency', 'Frequency', 'Monetary']].astype(float)
kmeans = KMeans(n_clusters=3, random_state=42)
rfm['RFM_Cluster'] = kmeans.fit_predict(X)

# Export
os.makedirs('data', exist_ok=True)
rfm.to_csv('data/customer_segments.csv', index=False)

print("✅ RFM segmentation complete. Output saved to 'data/customer_segments.csv'")


