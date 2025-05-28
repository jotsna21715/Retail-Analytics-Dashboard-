from faker import Faker
import random
import time
import json
import requests
import csv
import os

# Initialize Faker
fake = Faker()

# Sample product catalog with price ranges
product_catalog = {
    "Shoes": (50, 150),
    "Shirts": (20, 70),
    "Jeans": (40, 120),
    "Bags": (30, 100),
    "Watches": (80, 200)
}

# Endpoint to send data (FastAPI server will be built in Day 3)
API_ENDPOINT = "http://localhost:8000/transactions"

# Path to CSV log file
CSV_FILE = "data/sample_transactions.csv"

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

# Create CSV file and write headers if it doesn't exist
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["transaction_id", "customer_id", "product", "price", "timestamp"])

# Function to generate a fake transaction
def generate_transaction():
    product = random.choice(list(product_catalog.keys()))
    price_range = product_catalog[product]
    return {
        "transaction_id": fake.uuid4(),
        "customer_id": random.randint(1000, 2000),
        "product": product,
        "price": round(random.uniform(*price_range), 2),
        "timestamp": fake.iso8601()
    }

# Main function to simulate real-time streaming
def stream_transactions():
    print("📡 Streaming fake retail transactions...\nPress Ctrl+C to stop.\n")

    while True:
        txn = generate_transaction()

        # Print to console as pretty JSON
        print(json.dumps(txn, indent=2))

        # Log to CSV
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([txn["transaction_id"], txn["customer_id"], txn["product"], txn["price"], txn["timestamp"]])

        # Attempt to send to API
        try:
            response = requests.post(API_ENDPOINT, json=txn)
            print(f"→ Sent to API (status: {response.status_code})\n")
        except requests.exceptions.RequestException as e:
            print("⚠️  Failed to send to API:", e, "\n")

        # Wait before next transaction
        time.sleep(2)

# Run if this script is executed
if __name__ == "__main__":
    stream_transactions()
