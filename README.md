# FinVent
### Real-Time Event-Driven Fraud Detection Platform

---

## Executive Summary

Financial institutions process millions of transactions every day, where even a small delay in fraud detection can result in significant financial losses. Traditional batch-processing systems struggle to provide real-time insights and become difficult to scale as transaction volume increases.

**FinVent** is a real-time event-driven financial transaction processing system built using **Apache Kafka, Python, PostgreSQL, and Streamlit**.

Every incoming transaction is published as an event to Kafka and processed independently by multiple consumers. While one consumer persists transactions into PostgreSQL, another performs rule-based fraud detection and publishes suspicious transactions as new events. These alerts are stored separately and visualized through a live monitoring dashboard.

The project demonstrates how **Event-Driven Architecture (EDA)** enables loosely coupled, scalable services that react to financial events in real time.

---

# Business Problem

Banks and payment gateways continuously process thousands of financial transactions every second.

Traditional systems often process these transactions sequentially, making it difficult to:

- Detect fraud in real time
- Scale individual services independently
- Add new processing pipelines without modifying existing systems
- Monitor transaction activity continuously

The challenge is to build a system where multiple services can react to the same transaction independently without interfering with one another.

FinVent solves this using an **Event-Driven Architecture** powered by Apache Kafka.

---

# Solution Overview

Each transaction becomes an event.

Instead of sending the transaction directly to a database, it is published to a Kafka topic where multiple independent consumers process it simultaneously.

This architecture enables:

- Real-time transaction processing
- Independent fraud detection
- Decoupled storage services
- Easy system scalability
- Live operational dashboards

---

# System Architecture

```
                    Producer
                       │
                       ▼
             Kafka Topic : transactions
         ┌──────────────┼───────────────┐
         │                              │
         ▼                              ▼
 Storage Consumer               Fraud Consumer
         │                              │
         ▼                              ▼
 PostgreSQL                  Kafka Topic : fraud_alerts
         │                              │
         │                              ▼
         │                     Alert Consumer
         │                              │
         ▼                              ▼
 Transactions Table          Fraud Transactions Table
                 \            /
                  \          /
                   ▼        ▼
                 Streamlit Dashboard
```

---

# Event Flow

```
PaySim Dataset

        │

        ▼

Transaction Producer

        │

        ▼

Kafka Topic (transactions)

        │
 ┌──────┴─────────┐

 ▼                ▼

Storage      Fraud Detection

Consumer        Consumer

 │                │

 ▼                ▼

PostgreSQL   Kafka Topic (fraud_alerts)

                  │

                  ▼

           Alert Consumer

                  │

                  ▼

        Fraud Transactions Table

                  │

                  ▼

          Streamlit Dashboard
```

---

# Tech Stack

| Component | Technology |
|------------|------------|
| Event Streaming | Apache Kafka |
| Programming Language | Python |
| Database | PostgreSQL |
| Dashboard | Streamlit |
| Dataset | PaySim Financial Dataset |
| Architecture | Event-Driven Architecture |
| Messaging | Kafka Producer & Consumers |

---

# Project Structure

```
FinVent/

│
├── config/
│
├── consumer/
│   ├── storage_consumer.py
│   ├── fraud_consumer.py
│   └── alert_consumer.py
│
├── producer/
│   └── transaction_producer.py
│
├── dashboard/
│   └── app.py
│
├── database/
│
├── data/
│
├── requirements.txt
├── README.md
├── run_project.bat
└── stop_project.bat
```

---

# Fraud Detection Engine

Transactions are evaluated using a configurable rule-based scoring engine.

| Rule | Risk Score |
|------|-----------:|
| Large Transaction | +20 |
| Very Large Transaction | +20 |
| High Risk Transaction Type | +15 |
| Sender Balance Nearly Empty | +15 |
| Sender Account Fully Drained | +15 |
| Receiver Had Zero Balance | +10 |
| Round Amount Transaction | +10 |
| Frequent Transactions (Behavior Rule) | +20 |
| Dataset Flagged Fraud | +40 |

Transactions are classified into:

| Risk Level | Score |
|------------|------:|
| LOW | 0 – 34 |
| MEDIUM | 35 – 64 |
| HIGH | 65+ |

---

# Key Features

- Real-time financial transaction streaming
- Event-driven microservice architecture
- Multiple Kafka consumers processing the same event independently
- Rule-based fraud detection engine
- Behavioral fraud detection using transaction history
- PostgreSQL data persistence
- Live Streamlit monitoring dashboard
- Configurable fraud detection rules
- Decoupled processing pipeline

---

# Dashboard

The Streamlit dashboard provides real-time visibility into system activity.

### Features

- Total Transactions
- Total Transaction Volume
- Fraud Alerts
- Fraud Rate
- Risk Distribution
- Transaction Type Distribution
- Latest Fraud Alerts
- Live Transaction Monitoring

---

# Why Event-Driven Architecture?

Traditional transaction processing tightly couples multiple operations together.

FinVent separates these operations into independent services.

| Traditional System | FinVent (EDA) |
|--------------------|---------------|
| Sequential Processing | Parallel Event Processing |
| Tightly Coupled Services | Loosely Coupled Services |
| Difficult to Scale | Independently Scalable Consumers |
| Higher Processing Latency | Near Real-Time Processing |
| Single Processing Pipeline | Multiple Independent Pipelines |

Using Kafka enables new consumers to be added without changing existing services.

For example, future services could include:

- Email Notification Service
- SMS Alert Service
- Machine Learning Fraud Detection
- Audit Logging
- Real-Time Analytics
- Customer Notification Service

---

# Results

FinVent successfully demonstrates:

- Real-time event streaming using Apache Kafka
- Independent transaction storage and fraud detection pipelines
- Rule-based fraud scoring with configurable thresholds
- Event-driven communication between services
- Live fraud monitoring dashboard
- Decoupled architecture suitable for financial systems

---

# Future Improvements

- Machine Learning based fraud detection
- Docker Compose deployment
- Kubernetes orchestration
- Redis caching
- Cloud deployment (AWS)
- Prometheus & Grafana monitoring
- Email & SMS alert service
- Real-time analytics consumer
- REST API for external integrations

---

# Getting Started

### Clone Repository

```bash
git clone <repository-url>
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Start Kafka

```bash
# Start Kafka Server
```

### Run the Project

```bash
run_project.bat
```

Start the Producer separately.

```bash
python -m producer.transaction_producer
```

---
