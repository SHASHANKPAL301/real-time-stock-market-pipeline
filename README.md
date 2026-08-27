# Real-Time Stock Market Data Pipeline

An end-to-end real-time data engineering project that simulates stock market
data generation, streams events through Apache Kafka, processes and validates
the messages using a Python consumer, stores the data in PostgreSQL, and
visualizes the results using Power BI.

The entire data pipeline is containerized using Docker and Docker Compose.

---

## 1. Project Overview

The **Real-Time Stock Market Data Pipeline** is an event-driven data
engineering project built to demonstrate how streaming data can be generated,
processed, persisted, and visualized in near real time.

The pipeline simulates stock market events for multiple stock symbols and
continuously sends them through Kafka.

The Python consumer reads the Kafka messages, validates the data, and stores
valid records in PostgreSQL.

Power BI is then used to visualize the processed stock data.

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

# #Architecture
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
                    +------------------+
                             |
                             v
                  Automatic Page Refresh

**3. Technologies Used**


| Technology           | Purpose                            |
| -------------------- | ---------------------------------- |
| Python               | Data generation and Kafka consumer |
| Apache Kafka         | Real-time event streaming          |
| ZooKeeper            | Kafka coordination                 |
| PostgreSQL           | Persistent data storage            |
| Docker               | Containerization                   |
| Docker Compose       | Multi-container orchestration      |
| kafka-python         | Kafka client for Python            |
| psycopg2             | PostgreSQL connectivity            |
| JSON                 | Event/message format               |
| Power BI             | Data visualization                 |
| Power BI DirectQuery | Near-real-time database querying   |

**4. Project Structure**

real-time-data-pipeline/
|
+-- docker/
|   +-- compose.yaml
|
+-- producer/
|   +-- producer.py
|   +-- Dockerfile
|
+-- consumer/
|   +-- consumer.py
|   +-- Dockerfile
|
+-- README.md


5. Docker Services

The complete pipeline runs using Docker Compose.

ZooKeeper

ZooKeeper is used for Kafka coordination.

Image: confluentinc/cp-zookeeper:7.5.0
Port: 2181
Container: zookeeper
Kafka

Kafka acts as the message broker responsible for receiving and streaming
stock market events.

Image: confluentinc/cp-kafka:7.5.0
Port: 9092
Container: kafka

Kafka uses ZooKeeper for coordination.

KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181

Kafka listens on:

KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092

Kafka is advertised to other Docker services using:

KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092

The Docker service name kafka is used because the producer and consumer
communicate with Kafka through the Docker network.

Producer

The producer is a custom Python application running inside a Docker
container.

Container: producer
Application: producer.py
Consumer

The consumer is a custom Python application responsible for reading Kafka
messages, validating the data, and inserting valid records into PostgreSQL.

Container: consumer
Application: consumer.py
PostgreSQL

PostgreSQL is used as the persistent storage layer.

Image: postgres:16
Container: postgres
Port: 5432
Database: stockdb
User: stockuser
6. Docker Compose Configuration

The complete application is managed through:

docker/compose.yaml

The services are:

zookeeper
kafka
producer
consumer
postgres

The services use Docker's internal network and communicate using Docker
service names.

For example:

Producer -> kafka:9092
Consumer -> kafka:9092
Consumer -> postgres:5432
Restart Policy

The services use:

restart: unless-stopped

This provides automatic container recovery when a container exits unexpectedly,
while still allowing intentional manual stops.

7. Kafka Topic

The project uses a Kafka topic named:

stock-data

The topic can be verified using:

docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

Expected output:

stock-data
8. Python Producer

The Python producer generates synthetic stock market events.

The producer connects to Kafka using:

bootstrap_servers="kafka:9092"

The producer publishes messages to:

stock-data
Example Event
{
  "symbol": "AAPL",
  "price": 202.23
}
Dynamic Price Generation

The producer generates changing stock prices using Python's random module.

Example:

import random

Synthetic prices are generated using:

round(random.uniform(190, 210), 2)

Example generated prices:

197.42
203.18
199.73
206.11

A new stock event is generated periodically.

Important Note

The stock prices generated by this project are synthetic test data.

They do not represent actual market prices.

9. Kafka Producer Flow

The producer sends each event to Kafka:

producer.send("stock-data", message)
producer.flush()

The flow is:

Python Producer
       |
       | JSON event
       v
Kafka Broker
       |
       v
stock-data topic
10. Python Kafka Consumer

The Python consumer reads messages from the:

stock-data

Kafka topic.

The consumer uses the KafkaConsumer class from the kafka-python
library.

Kafka connection:

bootstrap_servers="kafka:9092"

The consumer receives JSON messages and converts them into Python dictionaries.

Example:

{
  "symbol": "AAPL",
  "price": 200
}

The consumer extracts:

symbol
price

from every valid message.

11. Consumer Dockerization

The consumer runs inside its own Docker container.

Structure:

consumer/
|
+-- consumer.py
|
+-- Dockerfile

The Dockerfile uses:

FROM python:3.11-slim

Required Python packages include:

kafka-python
psycopg2-binary

The consumer is executed using:

CMD ["python", "-u", "consumer.py"]

The -u option enables unbuffered Python output so that application logs are
visible immediately in Docker logs.

12. PostgreSQL Integration

PostgreSQL was added as the persistent storage layer of the pipeline.

PostgreSQL runs as a Docker container.

Image: postgres:16
Port: 5432
Database: stockdb
User: stockuser

Other Docker containers communicate with PostgreSQL using:

postgres:5432

because postgres is the Docker Compose service name.

13. PostgreSQL Database

The database is:

stockdb

The PostgreSQL credentials configured for the development environment are:

User: stockuser
Password: stockpass
Database: stockdb

For a production deployment, credentials should be stored using environment
variables or Docker secrets instead of committing them directly to source
control.

14. PostgreSQL Tables

The pipeline stores individual stock events in:

stock_data

The table contains fields such as:

Column	Type	Description
id	SERIAL	Unique record identifier
symbol	VARCHAR(10)	Stock symbol
price	DECIMAL(10,2)	Stock price
created_at	TIMESTAMP	Record creation time

The project also uses an aggregated table:

stock_summary

This table is used by Power BI for dashboard reporting.

15. Kafka Consumer → PostgreSQL

The Python consumer was extended to store received Kafka messages in
PostgreSQL.

The consumer establishes a PostgreSQL connection using psycopg2.

Example connection:

conn = psycopg2.connect(
    host="postgres",
    database="stockdb",
    user="stockuser",
    password="stockpass"
)

A database cursor is created:

cursor = conn.cursor()

When a valid Kafka message is received, the consumer inserts the data into
PostgreSQL.

Example:

INSERT INTO stock_data (symbol, price)
VALUES (%s, %s);

The transaction is committed using:

conn.commit()
Complete Flow
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
16. Data Validation

Data validation is performed before inserting Kafka messages into PostgreSQL.

Required Fields

Every message must contain:

symbol
price

Example validation:

if "symbol" not in data or "price" not in data:
    print("Invalid message: missing symbol or price")
    continue
Price Validation

The price must be numeric:

if not isinstance(data["price"], (int, float)):
    print("Invalid message: price must be numeric")
    continue
Symbol Validation

The symbol cannot be empty:

if not data["symbol"]:
    print("Invalid message: symbol cannot be empty")
    continue

Invalid messages are skipped and are not inserted into PostgreSQL.

17. Error Handling

Error handling was added to the Kafka consumer using try/except.

Database operations are protected so that a failed transaction does not leave
the PostgreSQL connection in an unusable transaction state.

If an error occurs:

conn.rollback()

The error is logged:

print(f"Error processing message: {e}")

The consumer can then continue processing subsequent messages.

18. Power BI Dashboard

Power BI is used as the visualization layer of the pipeline.

The dashboard reads the aggregated stock data from PostgreSQL.

The dashboard contains:

Total records by stock
Average stock price
Highest stock price
Lowest stock price
Highest vs lowest price comparison
Stock symbol filtering
KPI cards for stock-level metrics

Example stock symbols:

AAPL
AMZN
GOOGL
MSFT
19. Power BI DirectQuery

The final dashboard uses a PostgreSQL connection through DirectQuery.

This allows Power BI to query the current PostgreSQL data instead of relying
only on an imported snapshot.

The dashboard was tested with PostgreSQL while the pipeline was continuously
inserting new records.

20. Automatic Page Refresh

Automatic Page Refresh was enabled in the Power BI report.

This allows the report page to periodically query the PostgreSQL data through
DirectQuery.

The result is a near-real-time dashboard experience.

The complete visualization flow is:

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
21. Running the Project

Navigate to the Docker directory:

cd real-time-data-pipeline/docker

Start the complete pipeline:

docker compose up -d

Check running services:

docker compose ps

or:

docker ps

Expected containers:

zookeeper
kafka
producer
consumer
postgres

All services should be in the:

Up

state.

22. Checking Producer Logs

Producer logs can be checked using:

docker logs producer --tail 30

The producer should continuously generate stock events.

23. Checking Consumer Logs

Consumer logs can be checked using:

docker logs consumer --tail 30

Example output:

{'symbol': 'GOOGL', 'price': 204.33}
{'symbol': 'MSFT', 'price': 207.40}
{'symbol': 'AMZN', 'price': 191.73}
{'symbol': 'AAPL', 'price': 195.88}

This confirms that the consumer is receiving Kafka messages.

24. Verifying PostgreSQL Data

Connect to PostgreSQL using:

docker exec -it postgres psql -U stockuser -d stockdb

Check stock summary:

SELECT symbol, total_records
FROM public.stock_summary
ORDER BY symbol;

Example:

 symbol | total_records
--------+--------------
 AAPL   | 46180
 AMZN   | 1638
 GOOGL  | 1595
 MSFT   | 1615

The values continuously increase as new events are processed.

25. End-to-End Verification

The complete pipeline was tested successfully.

Test 1 — Kafka

Kafka topic verification:

docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

Result:

stock-data
Test 2 — Producer

Producer logs showed continuously generated stock events.

Test 3 — Consumer

Consumer logs showed successfully received Kafka messages.

Test 4 — PostgreSQL

PostgreSQL record counts increased while the producer and consumer were
running.

Test 5 — Power BI

The Power BI DirectQuery dashboard successfully reflected changing PostgreSQL
values through Automatic Page Refresh.

This confirms the end-to-end pipeline:

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
Power BI

is functioning successfully.

26. Reliability Configuration

Docker Compose services use:

restart: unless-stopped

This configuration was applied to:

zookeeper
kafka
producer
consumer
postgres

The configuration allows Docker to restart containers after unexpected
termination.

The restart policy can be verified using:

docker inspect producer \
  --format '{{.HostConfig.RestartPolicy.Name}}'

Expected output:

unless-stopped

The same check can be performed for the other services.

27. Troubleshooting
Issue 1 — Kafka Container Exited

Kafka initially exited unexpectedly.

Kafka logs showed a ZooKeeper-related error:

KeeperException$NodeExistsException

Kafka and ZooKeeper status were checked using:

docker compose ps -a

Kafka logs were inspected using:

docker compose logs kafka --tail 100

ZooKeeper was restarted when required:

docker compose restart zookeeper

Kafka was subsequently brought back to the Up state.

Issue 2 — Producer Kafka Timeout

The producer initially encountered:

KafkaTimeoutError:
Failed to update metadata after 60.0 secs

The issue was investigated by checking:

docker compose ps -a

and:

docker compose logs kafka --tail 100

Kafka connectivity was then verified using:

docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092

The stock-data topic was successfully returned and the producer was able to
communicate with Kafka.

Issue 3 — Container Restart Behavior

The Docker restart policy:

restart: unless-stopped

was added to the services.

Important behavior:

A manually stopped container is not automatically restarted by this policy.
The policy is intended to provide recovery from unexpected container exits.

28. Current Project Status
Component	Status
Docker	Completed
Docker Compose	Completed
ZooKeeper	Running
Kafka	Running
Kafka Topic	Completed
Python Producer	Working
Kafka Producer → Topic	Working
Python Consumer	Working
Consumer → PostgreSQL	Working
Data Validation	Completed
Error Handling	Completed
PostgreSQL	Working
Stock Summary	Working
Power BI	Completed
Power BI DirectQuery	Completed
Automatic Page Refresh	Completed
Docker Restart Policy	Completed
End-to-End Testing	Completed
29. Project Result

The project successfully demonstrates an end-to-end streaming data pipeline.

Synthetic stock market events are generated using Python and published to
Apache Kafka.

The Python consumer reads and validates these events before storing them in
PostgreSQL.

Power BI then queries the PostgreSQL data using DirectQuery and displays the
results through a near-real-time dashboard with Automatic Page Refresh.

Final Pipeline
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
30. Future Improvements

Possible future improvements include:

Replace synthetic stock prices with a real stock market API
Add event timestamps to every Kafka message
Improve Kafka partitioning strategy
Implement Kafka consumer groups
Add more advanced data quality checks
Add monitoring and alerting
Add centralized application logging
Improve dashboard design and analytics
Add CI/CD using GitHub Actions
Deploy the pipeline to a cloud platform
Introduce infrastructure-as-code
Add automated testing
Secure database credentials using environment variables or secrets
31. Key Learning Outcomes

This project provided hands-on experience with:

Event-driven architecture
Real-time data streaming
Apache Kafka
Kafka producers and consumers
Docker containerization
Docker Compose
PostgreSQL
Python database integration
Data validation
Transaction handling
Error handling
Power BI
DirectQuery
Automatic page refresh
Container restart policies
End-to-end pipeline testing
Troubleshooting distributed services
32. Development Approach

The project was developed incrementally.

The workflow followed:

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

Each major component was tested before moving to the next stage.

This approach helped identify and resolve issues related to Kafka connectivity,
ZooKeeper, producer communication, consumer processing, PostgreSQL storage,
and dashboard connectivity.

33. Conclusion

The Real-Time Stock Market Data Pipeline demonstrates how a complete
streaming data engineering workflow can be designed using open-source and
widely used technologies.

The project combines:

Python
+
Kafka
+
Docker
+
PostgreSQL
+
Power BI

to create an end-to-end pipeline capable of continuously processing and
visualizing stock market events.

The project is currently complete as a working development/portfolio project
and can be extended with real market APIs, cloud deployment, monitoring,
CI/CD, and additional analytics.