from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI()

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="retail",
    user="postgres",
    password="Jotsna@123",  # ⬅️ Replace with your actual password
    port="5432"
)
cursor = conn.cursor()

# Data model for incoming transactions
class Transaction(BaseModel):
    transaction_id: str
    customer_id: int
    product: str
    price: float
    timestamp: str

@app.post("/transactions")
async def receive_transaction(txn: Transaction):
    try:
        cursor.execute("""
            INSERT INTO transactions (transaction_id, customer_id, product, price, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            txn.transaction_id,
            txn.customer_id,
            txn.product,
            txn.price,
            txn.timestamp
        ))
        conn.commit()
        return {"status": "success", "transaction_id": txn.transaction_id}
    except Exception as e:
        print("⚠️ Database Error:", e)
        return {"status": "error", "detail": str(e)}
