# Financial Transactions Data Engineering Pipeline

> **⚠️ WORK IN PROGRESS**: This project is currently under active development and is not yet complete. Features and documentation may change significantly.

A comprehensive data engineering pipeline for processing financial transactions, with enriched user and merchant data, fraud detection capabilities, and economic context integration.

## Project Overview

This project simulates a real-world financial transactions processing system using modern data engineering practices. It follows an ELT (Extract, Load, Transform) approach, leveraging cloud technologies and event-driven architecture.

### Key Features

- **Rich Data Generation**: Realistic user, merchant, and transaction data with detailed attributes
- **Event Streaming**: Kafka-based event processing pipeline
- **Cloud Integration**: Google Cloud Storage and BigQuery for data warehousing
- **Economic Context**: Integration with FRED economic indicators API
- **Data Quality**: Schema validation and error handling
- **Flexible Storage**: PostgreSQL for operational data, BigQuery for analytics

## 📊 Data Models

### User Data
- Demographics (age, gender, occupation)
- Location information (country, city, coordinates)
- Financial attributes (credit score, risk score, income bracket)
- Preferences and spending patterns

### Merchant Data
- Business details (category, founding date, size)
- Location information
- Accepted payment methods
- Risk profile and fraud history

### Transaction Data
- Core transaction details (amount, currency, timestamp)
- Location context (online vs. in-person)
- Payment method details
- Transaction type and status

### Economic Indicators
- Unemployment rates
- Consumer Price Index
- GDP growth
- Federal funds rate
- Consumer sentiment

## Architecture

```
┌─────────────┐    ┌─────────┐    ┌───────────────┐    ┌─────────────┐
│  Producers  │───▶│  Kafka  │───▶│   Consumers   │───▶│ Google Cloud│
└─────────────┘    └─────────┘    └───────────────┘    └─────────────┘
      │                                                       ▲
      │                                                       │
      │               ┌─────────────┐                         │
      └──────────────▶│ PostgreSQL  │─────────────────────────┘
                      └─────────────┘
```

### Components

1. **Producers**:
   - User data generator
   - Merchant data generator
   - Transaction simulator
   - Economic indicators service

2. **Consumers**:
   - Fraud detection service
   - Data loading to BigQuery
   - Economic indicators consumer

3. **Storage**:
   - PostgreSQL for operational data
   - Google Cloud Storage for data lake
   - BigQuery for data warehouse

## 🛠️ Technologies Used

- **Python**: Core application logic
- **Apache Kafka**: Event streaming
- **PostgreSQL**: Operational database
- **Google Cloud Storage**: Data lake
- **Google BigQuery**: Data warehouse
- **Avro**: Schema definition and serialization
- **FRED API**: Economic data integration
- **Faker**: Synthetic data generation

## 🚦 Getting Started

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Google Cloud account
- Confluent Kafka account (or local Kafka setup)
- PostgreSQL

### Environment Setup

1. Clone the repository
2. Create a `.env` file based on `.env.example`
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

1. Start the PostgreSQL database:
   ```bash
   docker-compose up -d postgres
   ```

2. Initialize the database schema:
   ```bash
   python -m src.utils.postgres_db
   ```

3. Start the producers:
   ```bash
   python -m src.producers.user_producer
   python -m src.producers.merchant_producer
   python -m src.producers.transaction_producer
   python -m src.services.economic_indicators_service
   ```

4. Start the consumers:
   ```bash
   python -m src.consumers.economic_indicators_consumer
   ```

## 📝 Project Structure

```
Financial-Transactions-DE/
├── src/
│   ├── producers/            # Data generation
│   ├── consumers/            # Data processing
│   ├── services/             # External services integration
│   ├── utils/                # Shared utilities
│   ├── schemas/              # Avro schemas
│   └── etl/                  # ELT transformations
├── DWH/                      # Data warehouse artifacts
├── airflow/                  # Airflow configuration
├── config/                   # Configuration files
├── dags/                     # Airflow DAGs
├── data_contracts/           # Data contracts and specifications
├── jars/                     # Java dependencies
├── docker-compose.yml        # Docker configuration
└── .env                      # Environment variables
```

## Acknowledgements
- Federal Reserve Economic Data (FRED) for economic indicators
- Faker library for synthetic data generation
- Confluent Kafka for event streaming capabilities
- Google Cloud for data warehousing solutions
