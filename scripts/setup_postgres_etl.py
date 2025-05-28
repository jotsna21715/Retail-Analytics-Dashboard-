import psycopg2

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="retail",
    user="postgres",
    password="Jotsna@123",  # Replace this
    port="5432"
)

cursor = conn.cursor()

# Create the transactions table
cursor.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR PRIMARY KEY,
    customer_id INT NOT NULL,
    product VARCHAR(100),
    price FLOAT,
    timestamp TIMESTAMP
);
""")

# Create index for faster timestamp queries
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions (timestamp);
""")

conn.commit()
cursor.close()
conn.close()

print("✅ PostgreSQL database and transactions table are ready.")
