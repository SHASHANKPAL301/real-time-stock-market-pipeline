# Real-Time Stock Market Data Pipeline

An end-to-end real-time data engineering project that simulates stock market data generation, streams events through Apache Kafka, processes and validates messages using a Python consumer, stores the data in PostgreSQL, and visualizes the results using Power BI.

The entire data pipeline is containerized using Docker and Docker Compose.

---

## 1. Project Overview

The **Real-Time Stock Market Data Pipeline** is an event-driven data engineering project built to demonstrate how streaming data can be generated, processed, persisted, and visualized in near real time.

The pipeline simulates stock market events for multiple stock symbols and continuously sends them through Kafka.

The Python consumer reads Kafka messages, validates the data, and stores valid records in PostgreSQL.

Power BI is then used to visualize the processed stock data through an interactive dashboard.

### Complete Pipeline

```text
Python Producer
       |
       v
Apache Kafka
       |
       | stock-data topic
       v
Python Consumer
       |
       | Data Validation
       v
PostgreSQL
       |
       v
stock_summary
       |
       v
Power BI Dashboard
       |
       v
Automatic Page Refresh
```

---

## 2. Architecture

```text
                         Stock Data
                             |
                             v
                    +------------------+
                    |  Python Producer |
                    +--------+---------+
                             |
                             | JSON events
                             v
                    +------------------+
                    |      Kafka       |
                    |   stock-data     |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    |  Python Consumer |
                    +--------+---------+
                             |
                             | Validation
                             v
                    +------------------+
                    |   PostgreSQL     |
                    |    stock_data    |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    |  stock_summary   |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    |     Power BI     |
                    |    Dashboard     |
                    +--------+---------+
                             |
                             v
                    Automatic Page Refresh
```

### Supporting Infrastructure

```text
Docker Compose
|
+-- ZooKeeper
+-- Kafka
+-- Producer
+-- Consumer
+-- PostgreSQL
```

---

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data generation and Kafka consumer |
| Apache Kafka | Real-time event streaming |
| ZooKeeper | Kafka coordination |
| PostgreSQL | Persistent data storage |
| Docker | Containerization |
| Docker Compose | Multi-container orchestration |
| kafka-python | Kafka client for Python |
| psycopg2 | PostgreSQL connectivity |
| JSON | Event/message format |
| Power BI | Data visualization |
| Power BI DirectQuery | Near-real-time database querying |

---

## 4. Project Structure

```text
real-time-data-pipeline/
|
+-- docker/
|   +-- compose.yaml
|
+-- producer/
|   +-- producer.py
|   +-- Dockerfile
|   +-- requirements.txt
|
+-- consumer/
|   +-- consumer.py
|   +-- Dockerfile
|
+-- docs/
|   +-- dashboard.png
|
+-- README.md
+-- requirements.txt
+-- .gitignore
```

---

## 5. Docker Services

The complete application is managed using Docker Compose.

### ZooKeeper

ZooKeeper is used for Kafka coordination.

```text
Image: confluentinc/cp-zookeeper:7.5.0
Container: zookeeper
Port: 2181
```

### Kafka

Kafka acts as the message broker responsible for receiving and streaming stock market events.

```text
Image: confluentinc/cp-kafka:7.5.0
Container: kafka
Port: 9092
```

Kafka connects to ZooKeeper using:

```yaml
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
```

Kafka listens on:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
```

Kafka is advertised to other Docker services using:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
```

The Docker service name `kafka` is used by the producer and consumer to communicate with the Kafka broker through the Docker network.

### Producer

The producer is a custom Python application running inside a Docker container.

```text
Container: producer
Application: producer.py
```

### Consumer

The consumer is a custom Python application that reads Kafka messages, validates the data, and stores valid records in PostgreSQL.

```text
Container: consumer
Application: consumer.py
```

### PostgreSQL

PostgreSQL is used as the persistent storage layer.

```text
Image: postgres:16
Container: postgres
Port: 5432
Database: stockdb
User: stockuser
```

---

## 6. Kafka Topic

The project uses a Kafka topic named:

```text
stock-data
```

The topic can be verified using:

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

Expected output:

```text
stock-data
```

---

## 7. Python Producer

The Python producer generates synthetic stock market events.

The producer connects to Kafka using:

```python
bootstrap_servers="kafka:9092"
```

Stock events are published to the `stock-data` Kafka topic.

### Example Event

```json
{
  "symbol": "AAPL",
  "price": 202.23
}
```

### Dynamic Price Generation

The producer uses Python's `random` module to generate changing stock prices.

```python
import random
```

Synthetic prices are generated using:

```python
round(random.uniform(190, 210), 2)
```

Example values:

```text
197.42
203.18
199.73
206.11
```

> The generated stock prices are synthetic test data and do not represent actual market prices.

---

## 8. Python Kafka Consumer

The Python consumer reads messages from the `stock-data` Kafka topic.

The consumer uses the `KafkaConsumer` class from the `kafka-python` library.

Kafka connection:

```python
bootstrap_servers="kafka:9092"
```

The consumer receives JSON messages and converts them into Python dictionaries.

Example:

```json
{
  "symbol": "AAPL",
  "price": 200
}
```

The consumer extracts the `symbol` and `price` fields from each message.

---

## 9. Consumer Dockerization

The consumer runs inside its own Docker container.

Structure:

```text
consumer/
|
+-- consumer.py
+-- Dockerfile
```

The Dockerfile uses Python 3.11:

```dockerfile
FROM python:3.11-slim
```

Required Python packages include:

```text
kafka-python
psycopg2-binary
```

The consumer is executed using:

```dockerfile
CMD ["python", "-u", "consumer.py"]
```

The `-u` option ensures that Python output is written immediately, making application logs visible through Docker.

---

## 10. PostgreSQL Integration

PostgreSQL acts as the persistent storage layer of the pipeline.

Configuration:

```text
Image: postgres:16
Port: 5432
Database: stockdb
User: stockuser
```

Other Docker services communicate with PostgreSQL using:

```text
postgres:5432
```

because `postgres` is the Docker Compose service name.

---

## 11. PostgreSQL Database and Tables

The database used by the project is:

```text
stockdb
```

Development credentials:

```text
User: stockuser
Password: stockpass
Database: stockdb
```

> For production deployments, database credentials should be stored using environment variables, Docker secrets, or another secure secret-management solution instead of being committed to source control.

The pipeline stores individual stock events in:

```text
stock_data
```

Example table structure:

| Column | Type | Description |
|---|---|---|
| id | SERIAL | Unique record identifier |
| symbol | VARCHAR(10) | Stock symbol |
| price | DECIMAL(10,2) | Stock price |
| created_at | TIMESTAMP | Record creation time |

The project also uses:

```text
stock_summary
```

for aggregated reporting data used by Power BI.

---

## 12. Kafka Consumer → PostgreSQL

The Python consumer was extended to store received Kafka messages in PostgreSQL.

The consumer establishes a PostgreSQL connection using `psycopg2`.

```python
conn = psycopg2.connect(
    host="postgres",
    database="stockdb",
    user="stockuser",
    password="stockpass"
)
```

A database cursor is created:

```python
cursor = conn.cursor()
```

Valid Kafka messages are inserted into PostgreSQL:

```sql
INSERT INTO stock_data (symbol, price)
VALUES (%s, %s);
```

The transaction is committed using:

```python
conn.commit()
```

### Processing Flow

```text
Kafka
  |
  v
Python Consumer
  |
  v
JSON Parsing
  |
  v
Data Validation
  |
  v
PostgreSQL INSERT
  |
  v
Commit
  |
  v
stock_data
```

---

## 13. Data Validation

Data validation is performed before inserting messages into PostgreSQL.

### Required Fields

Each message must contain:

```text
symbol
price
```

Validation example:

```python
if "symbol" not in data or "price" not in data:
    print("Invalid message: missing symbol or price")
    continue
```

### Price Validation

The price must be numeric:

```python
if not isinstance(data["price"], (int, float)):
    print("Invalid message: price must be numeric")
    continue
```

### Symbol Validation

The stock symbol cannot be empty:

```python
if not data["symbol"]:
    print("Invalid message: symbol cannot be empty")
    continue
```

Invalid messages are skipped and are not inserted into PostgreSQL.

---

## 14. Error Handling

The Kafka consumer processes messages using `try/except`.

If a database error occurs, the current transaction is rolled back:

```python
conn.rollback()
```

The error is logged:

```python
print(f"Error processing message: {e}")
```

This prevents a failed database transaction from leaving the connection in a failed state and allows the consumer to continue processing subsequent messages.

---

## 15. Power BI Dashboard

Power BI is used as the visualization layer of the pipeline.

The dashboard connects to PostgreSQL reporting data and provides an interactive view of the stock pipeline.

### Dashboard Features

- Stock symbol slicer
- Total records
- Average stock price
- Highest stock price
- Lowest stock price
- Highest vs lowest price comparison
- Average stock price by symbol
- Interactive filtering
- DirectQuery
- Automatic Page Refresh

### Supported Stock Symbols

```text
AAPL
AMZN
GOOGL
MSFT
```

### Dashboard Preview

![Real-Time Stock Market Dashboard](docs/dashboard.png)

---

## 16. Power BI DirectQuery

The Power BI report uses a PostgreSQL connection through **DirectQuery**.

DirectQuery allows Power BI to query the current PostgreSQL data instead of relying only on an imported snapshot.

This makes the dashboard suitable for near-real-time reporting when combined with Automatic Page Refresh.

### Visualization Flow

```text
Producer
   |
   v
Kafka
   |
   v
Consumer
   |
   v
PostgreSQL
   |
   v
Power BI DirectQuery
   |
   v
Automatic Page Refresh
```

---

## 17. Running the Project

Navigate to the Docker directory:

```bash
cd real-time-data-pipeline/docker
```

Start all services:

```bash
docker compose up -d
```

Check the running services:

```bash
docker compose ps
```

or:

```bash
docker ps
```

Expected containers:

```text
zookeeper
kafka
producer
consumer
postgres
```

All services should be in the `Up` state.

---

## 18. Checking Producer Logs

Producer logs can be viewed using:

```bash
docker logs producer --tail 30
```

The producer should continuously generate stock events.

---

## 19. Checking Consumer Logs

Consumer logs can be viewed using:

```bash
docker logs consumer --tail 30
```

Example:

```text
{'symbol': 'GOOGL', 'price': 204.33}
{'symbol': 'MSFT', 'price': 207.40}
{'symbol': 'AMZN', 'price': 191.73}
{'symbol': 'AAPL', 'price': 195.88}
```

This confirms that the consumer is successfully receiving Kafka messages.

---

## 20. Verifying PostgreSQL Data

Connect to PostgreSQL:

```bash
docker exec -it postgres psql -U stockuser -d stockdb
```

Check the aggregated stock data:

```sql
SELECT symbol, total_records
FROM public.stock_summary
ORDER BY symbol;
```

Example output:

```text
 symbol | total_records
--------+--------------
 AAPL   | 46180
 AMZN   | 1638
 GOOGL  | 1595
 MSFT   | 1615
```

The record counts increase as new events are processed.

---

## 21. End-to-End Verification

The pipeline was tested component by component.

### Kafka Verification

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

Expected result:

```text
stock-data
```

### Producer Verification

Producer logs confirmed that stock events were continuously generated.

### Consumer Verification

Consumer logs confirmed that Kafka messages were successfully received and processed.

### PostgreSQL Verification

PostgreSQL record counts increased as new stock events were consumed.

### Power BI Verification

The Power BI DirectQuery dashboard successfully displayed PostgreSQL data and reflected updated values through Automatic Page Refresh.

### Final Verified Flow

```text
Python Producer
       |
       v
Apache Kafka
       |
       v
stock-data
       |
       v
Python Consumer
       |
       v
Data Validation
       |
       v
PostgreSQL
       |
       v
stock_summary
       |
       v
Power BI DirectQuery
       |
       v
Automatic Page Refresh
```

---

## 22. Docker Restart Policy

The Docker Compose services use:

```yaml
restart: unless-stopped
```

This allows containers to restart automatically after unexpected termination.

The restart policy can be verified using:

```bash
docker inspect producer \
  --format '{{.HostConfig.RestartPolicy.Name}}'
```

Expected output:

```text
unless-stopped
```

---

## 23. Troubleshooting

### Kafka Container Exited

Kafka initially exited unexpectedly.

Kafka logs showed a ZooKeeper-related error:

```text
KeeperException$NodeExistsException
```

The Docker services were inspected using:

```bash
docker compose ps -a
```

Kafka logs were inspected using:

```bash
docker compose logs kafka --tail 100
```

ZooKeeper was restarted when required:

```bash
docker compose restart zookeeper
```

Kafka was subsequently brought back to the `Up` state.

### Producer Kafka Timeout

The producer initially encountered:

```text
KafkaTimeoutError:
Failed to update metadata after 60.0 secs
```

The issue was investigated using:

```bash
docker compose ps -a
```

and:

```bash
docker compose logs kafka --tail 100
```

Kafka connectivity was then verified using:

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

The `stock-data` topic was successfully returned and the producer was able to communicate with Kafka.

---

## 24. Current Project Status

| Component | Status |
|---|---|
| Docker | Completed |
| Docker Compose | Completed |
| ZooKeeper | Working |
| Kafka | Working |
| Kafka Topic | Completed |
| Python Producer | Working |
| Producer → Kafka | Working |
| Python Consumer | Working |
| Consumer → PostgreSQL | Working |
| Data Validation | Completed |
| Error Handling | Completed |
| PostgreSQL | Working |
| Stock Summary | Working |
| Power BI Dashboard | Completed |
| Power BI DirectQuery | Completed |
| Automatic Page Refresh | Completed |
| Docker Restart Policy | Completed |
| End-to-End Testing | Completed |
| GitHub Documentation | Completed |

---

## 25. Project Result

The project successfully demonstrates an end-to-end event-driven data pipeline.

Synthetic stock market events are generated using Python and published to Apache Kafka.

The Python consumer reads and validates these events before storing them in PostgreSQL.

Power BI then queries the PostgreSQL reporting data using DirectQuery and displays the results through an interactive dashboard with Automatic Page Refresh.

### Final Architecture

```text
Python Producer
       |
       v
Apache Kafka
       |
       v
stock-data
       |
       v
Python Consumer
       |
       v
Data Validation
       |
       v
PostgreSQL
       |
       v
stock_summary
       |
       v
Power BI DirectQuery
       |
       v
Automatic Page Refresh
```

---

## 26. Future Improvements

Possible future improvements include:

- Replace synthetic stock prices with a real stock market API
- Add event timestamps to Kafka messages
- Implement Kafka partitions
- Implement Kafka consumer groups
- Add advanced data quality checks
- Add monitoring and alerting
- Add centralized logging
- Improve Power BI analytics
- Add CI/CD using GitHub Actions
- Deploy the pipeline to a cloud platform
- Introduce Infrastructure as Code
- Add automated testing
- Secure database credentials using environment variables or secrets

---

## 27. Key Learning Outcomes

This project provided hands-on experience with:

- Event-driven architecture
- Real-time data streaming
- Apache Kafka
- Kafka producers and consumers
- Docker containerization
- Docker Compose
- PostgreSQL
- Python database integration
- Data validation
- Transaction handling
- Error handling
- Power BI
- DirectQuery
- Automatic Page Refresh
- Container restart policies
- End-to-end pipeline testing
- Distributed service troubleshooting

---

## 28. Development Approach

The project was developed incrementally using the following workflow:

```text
Build
  ↓
Run
  ↓
Test
  ↓
Troubleshoot
  ↓
Verify
  ↓
Document
  ↓
Continue
```

Each major component was tested before moving to the next stage.

This approach helped identify and resolve issues related to Kafka connectivity, ZooKeeper, producer communication, consumer processing, PostgreSQL storage, Power BI connectivity, and dashboard refresh.

---

## 29. Conclusion

The **Real-Time Stock Market Data Pipeline** demonstrates how a complete streaming data engineering workflow can be built using Python, Apache Kafka, Docker, PostgreSQL, and Power BI.

The project combines:

```text
Python
+
Apache Kafka
+
Docker
+
PostgreSQL
+
Power BI
```

to create an end-to-end pipeline capable of continuously processing, storing, and visualizing stock market events.

The project can be further extended with real market APIs, cloud deployment, monitoring, CI/CD, and advanced analytics.
