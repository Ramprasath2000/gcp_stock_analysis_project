import json
import time
import random
from faker import Faker
from google.cloud import pubsub_v1
from datetime import datetime, timezone

fake = Faker()

PROJECT_ID = "stock-pipeline-project-499806"
TOPIC_ID = "stock-transactions-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

SYMBOLS = ["RELIANCE", "TCS", "INFY", "WIPRO", "HDFC"]
EXCHANGE = ["NSE", "BSE"]
REGIONS =["Mumbai", "Bangalore", "Chennai", "Delhi", "Hyderabad"]

def generate_trade():
    return {
        "transaction_id": str(fake.uuid4()),
        "symbol": random.choice(SYMBOLS),
        "price": round(random.uniform(500, 3000), 2),
        "volume": random.randint(1, 500),
        "trade_type" : random.choice(["BUY", "SELL"]),
        "trader_id": fake.user_name(),
        "exchange": random.choice(EXCHANGE),
        "region": random.choice(REGIONS),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

while True:
    trade = generate_trade()
    message = json.dumps(trade).encode("utf-8") 
    publisher.publish(topic_path, message)
    print(f"Published: {trade['symbol']} at {trade['price']}")
    time.sleep(1)
