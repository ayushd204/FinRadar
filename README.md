# FinRadar

A real-time credit card fraud detection system built around event-driven transaction processing.

FinRadar simulates realistic credit card transactions, evaluates them using configurable fraud detection rules, and sends real time mail alerts on detection of fraud.

## Overview

Financial institutions need to detect suspicious transactions quickly and accurately. FinRadar simulates a development platform for experimenting with real-time fraud detection workflows.

The system currently includes a Kafka-based transaction producer that:

- Generates realistic customers and merchants
- Simulates customer spending behavior
- Creates credit card transactions
- Applies configurable fraud detection rules
- Assigns fraud scores and detection reasons


## Architecture



```

## Getting Started

### Prerequisites

Before running FinRadar, make sure you have:

- Python 3.10 or later
- Access to an Apache Kafka cluster
- A Kafka topic for transaction events
- Kafka credentials with permission to publish messages

The producer is designed to work with Confluent Cloud or another Kafka-compatible service.

### Installation

Clone the repository:

```bash
git clone https://github.com/ayushd204/FinRadar.git
cd FinRadar/kafka_producer
```

Create and activate a virtual environment:

#### macOS/Linux

```bash
python -m venv .venv
source .venv/bin/activate
```

#### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the `kafka_producer` directory.

```env
BOOTSTRAP_SERVERS=your-bootstrap-servers:9092
API_KEY=your_api_key
API_SECRET=your_api_secret
TOPIC_NAME=credit_card_transactions

TRANSACTIONS_PER_SECOND=5
FRAUD_PERCENTAGE=0.08
TOTAL_CUSTOMERS=1000
TOTAL_MERCHANTS=200
RANDOM_SEED=42
```

### Configuration Options

| Variable | Description | Example |
|---|---|---|
| `BOOTSTRAP_SERVERS` | Kafka bootstrap server address | `your-cluster:9092` |
| `API_KEY` | Kafka API key | `your_api_key` |
| `API_SECRET` | Kafka API secret | `your_api_secret` |
| `TOPIC_NAME` | Kafka topic for transaction events | `credit_card_transactions` |
| `TRANSACTIONS_PER_SECOND` | Number of transactions generated per second | `5` |
| `FRAUD_PERCENTAGE` | Approximate percentage of generated fraudulent transactions | `0.08` |
| `TOTAL_CUSTOMERS` | Number of simulated customers | `1000` |
| `TOTAL_MERCHANTS` | Number of simulated merchants | `200` |
| `RANDOM_SEED` | Seed for reproducible data generation | `42` |

> Never commit `.env` files, API keys, or other credentials to source control.

## Running the Producer

From the `kafka_producer` directory, run:

```bash
python producer.py
```

The producer will:

1. Generate customer and merchant datasets if they do not already exist.
2. Create simulated credit card transactions.
3. Evaluate each transaction using the fraud detection engine.
4. Publish transaction events to the configured Kafka topic.
5. Log delivery results and periodic runtime statistics.

Stop the producer with:

```text
Ctrl+C
```

The application handles graceful shutdown when interrupted.

## Example Output

```text
INFO - TXN000001 | Amount=1450.50 | Merchant=Amazon India | Fraud=15 | Reason=NORMAL | Offset=None
INFO - TXN000002 | Amount=9500.00 | Merchant=Delhi Grocers | Fraud=45 | Reason=VELOCITY_FRAUD | Offset=12
```

## Example Transaction Event

A transaction published to Kafka follows a JSON structure similar to:

```json
{
  "transaction_id": "TXN000001",
  "customer_id": "CUST000123",
  "merchant_id": "MERCHANT000045",
  "amount": 1450.50,
  "currency": "INR",
  "transaction_type": "online",
  "fraud_score": 15,
  "is_fraud": false,
  "fraud_reason": "NORMAL",
  "timestamp": "2025-01-01T12:00:00Z"
}
```

The exact event fields may change as the project evolves.

## Fraud Detection

FinRadar supports configurable fraud scoring and detection rules. Example fraud signals include:

- Unusually large transaction amounts
- Spending behavior that differs from a customer’s normal pattern



## Security

This project is intended for development, experimentation, and demonstration purposes.

Do not use the simulator or fraud rules as a production financial security system without additional:

- Security review
- Data validation
- Access controls
- Monitoring and alerting
- Compliance review
- Load testing
- Privacy protections
- Failure recovery procedures
