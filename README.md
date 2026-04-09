# Event Processing Platform

## Overview

This project implements a distributed event-driven microservices architecture using Python, Docker, and Apache Kafka to process and analyze real-time game events. The system consists of multiple services (receiver, storage, analyzer, processor, and dashboard) that communicate asynchronously through Kafka, with MySQL providing persistent data storage. It is designed for scalability and fault tolerance, incorporating service startup coordination, Kafka retry logic, and database connection pooling. Centralized configuration files are used to manage service settings consistently, and Docker volumes ensure data persistence across container restarts.

---

## Architecture

The platform consists of the following services:

* **Receiver**
  Accepts HTTP requests and publishes events to Kafka.

* **Storage**
  Consumes Kafka events and persists them in MySQL.

* **Analyzer**
  Reads event data and exposes query endpoints.

* **Processor**
  Performs background processing and aggregation tasks.

* **Dashboard**
  Frontend UI for visualizing and interacting with system data.

* **Kafka + Zookeeper**
  Handles event streaming and message brokering.

* **MySQL Database**
  Provides persistent storage for processed events.

---

## Technologies Used

* Python (Flask / Connexion)
* Docker & Docker Compose
* Apache Kafka
* MySQL
* SQLAlchemy
* REST APIs

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd <your-project>
```

### 2. Run the system

```bash
docker compose up --build
```

### 3. Access services

* Receiver: http://localhost:8080/ui
* Storage: http://localhost:8090/ui
* Processor: http://localhost:8100/ui
* Analyzer: http://localhost:8110/ui
* Dashboard: http://localhost

---

## How It Works

1. Client sends events to the **Receiver**
2. Receiver publishes events to **Kafka**
3. **Storage** consumes events and stores them in MySQL
4. **Analyzer** provides query endpoints for event data
5. **Processor** performs background computations
6. **Dashboard** visualizes and interacts with system data

---

## Key Features

### Reliable Startup

* Services wait for dependencies using startup scripts
* Prevents race conditions during container initialization

### Fault Tolerance

* Kafka producer and consumer retry logic
* Automatic recovery from broker failures

### Database Reliability

* SQLAlchemy connection pooling:

  * `pool_pre_ping`
  * `pool_recycle`
  * `pool_size`

### Data Consistency

* Ensures all events are:

  * Received
  * Stored
  * Processed
  * Queryable

### Centralized Configuration

* Configuration managed through `/config` directory
* Consistent service configuration across all components

### Persistent Storage

* Docker volumes used for MySQL and Kafka
* Prevents data loss across container restarts

---

## API Endpoints

### Receiver

* `POST /player_snapshots`
* `POST /match_events`

### Analyzer

* `GET /player_snapshots/{index}`
* `GET /match_events/{index}`
* `GET /stats`

### Storage

* `GET /player_snapshots`
* `GET /match_events`

---

## Testing

You can test the system using:

* Postman
* curl
* jMeter (for load testing)

Example:

```bash
curl -X POST http://localhost:8080/player_snapshots \
-H "Content-Type: application/json" \
-d '{ ... }'
```

---

Project Structure
.
├── Analyzer/        # Kafka consumer for querying event data
├── Receiver/        # API service that receives and publishes events to Kafka
├── Storage/         # Consumes events and stores them in MySQL
├── Processor/       # Background processing and aggregation service
├── dashboard-ui/    # Frontend dashboard for visualization
├── config/          # Centralized configuration files
├── data/            
├── logs/            # Service logs
├── docker-compose.yml
├── README.md

---

## Future Improvements

* Authentication and authorization
* Improved dashboard UI/UX
* Monitoring and logging (e.g., Prometheus, Grafana)
* Kafka partition scaling and optimization
* Load balancing across services

---

## Author

* Joseph Oh

