from kafka import KafkaProducer
import json
import time
import random


producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    api_version=(3, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

stocks = ["AAPL", "GOOGL", "MSFT", "AMZN"]

while True:
    symbol=random.choice(stocks)
    message = {
        "symbol": symbol,
        "price": round(random.uniform(190,210),2)
    }

    producer.send("stock-data", message)
    producer.flush()

    print(message)

    time.sleep(5)