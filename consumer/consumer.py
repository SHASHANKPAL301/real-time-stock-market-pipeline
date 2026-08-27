from kafka import KafkaConsumer
import json
import psycopg2

conn = psycopg2.connect(
    host="postgres",
    database="stockdb",
    user="stockuser",
    password="stockpass"
)

cursor = conn.cursor()

consumer = KafkaConsumer(
    "stock-data",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest"
)

for message in consumer:
    try:
        data = json.loads(message.value.decode("utf-8"))
        print(data)
        if "symbol" not in data or "price" not in data:
            print("Invalid message: missing symbol or price")
            continue
        if not isinstance(data["price"],(int,float)):
            print("Invalid message: price must be numeric")
            continue
        if not data["symbol"]:
            print("Invalid message: symbol cannot be empty")
            continue
        cursor.execute(
                    "INSERT INTO stock_data (symbol, price) VALUES (%s, %s)",
                    (data["symbol"], data["price"])
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error processing message : {e}")
        
