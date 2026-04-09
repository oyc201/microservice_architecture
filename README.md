# Event Processing Platform

## Overview

This project is a distributed event-driven system built using microservices. It processes game-related events (player snapshots and match events) using Kafka, stores them in a database, and provides analytics through an API.

The system is designed to be:

* Scalable
* Fault-tolerant
* Resilient to service failures

---

## Architecture

The platform consists of the following services:

* **Receiver**
  Accepts incoming HTTP requests and publishes events to Kafka.

* **Storage**
  Consumes Kafka events and stores them in a MySQL database.

* **Analyzer**
  Reads events from Kafka and provides query endpoints.

* **Processor**
  Performs periodic processing tasks on stored data.

* **Dashboard**
  Frontend UI for interacting with the system.

* **Kafka + Zookeeper**
  Message broker for event streaming.

* **MySQL Database**
  Persistent storage for events.

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

### 2. Build and run services

```bash
docker compose up --build
```

---

## How It Works

1. Client sends events to the **Receiver**
2. Receiver publishes events to **Kafka**
3. **Storage** consumes events and writes to database
4. **Analyzer** reads events for querying
5. **Processor** performs background processing tasks

---

## Key Features

### Reliable Startup

* Implemented service dependency handling using wait scripts
* Ensures services only start when dependencies are ready

### Kafka Retry Logic

* Producer and consumer wrappers handle reconnection automatically
* System continues working even if Kafka restarts

### Database Resilience

* SQLAlchemy connection pooling configured:

  * `pool_pre_ping`
  * `pool_recycle`
  * `pool_size`

### Data Consistency

* Ensures all events sent are:

  * Stored in database
  * Processed correctly
  * Available in analyzer

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

You can use tools like:

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

## Project Structure

```bash
.
├── Receiver/
├── Storage/
├── Analyzer/
├── Processor/
├── dashboard-ui/
├── config/
├── docker-compose.yml
```

---

## Notes

* Config files are located in `/config`
* Services communicate via Docker network

---

## Future Improvements

* Add authentication
* Improve dashboard UI
* Add monitoring/log aggregation
* Optimize Kafka partitioning
* Implement sensitive configuration with .env file

---

## Author

* Josepoh Oh
