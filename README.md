# Chronos Data Platform

Enterprise data engineering and analytics platform meta-repository.

## Project Structure

```
chronos-data-platform/
├── src/
│   ├── ingestion/          # Data ingestion pipelines
│   │   ├── batch/          # Batch ingestion
│   │   └── streaming/      # Streaming ingestion
│   ├── processing/         # Data transformation
│   │   ├── etl/           # ETL pipelines
│   │   └── features/      # Feature engineering
│   ├── storage/           # Data lake and warehouse
│   │   ├── bronze/        # Raw data
│   │   ├── silver/        # Cleaned data
│   │   └── gold/          # Aggregated data
│   ├── orchestration/    # Pipeline orchestration
│   └── quality/          # Data quality checks
├── dbt/                 # dbt transformations
├── notebooks/           # Data exploration
└── tests/              # Test files
```

## Technology Stack

- **Ingestion**: Azure Event Hubs, Kafka, Airbyte, Debezium
- **Processing**: Spark (Databricks), Flink, dbt
- **Storage**: Delta Lake (ADLS), Azure Synapse, Snowflake
- **Orchestration**: Azure Data Factory, Airflow
- **Quality**: Great Expectations, Soda

## Getting Started

### Prerequisites
- Python 3.11+
- Docker
- Azure subscription

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up local Spark:
```bash
docker-compose up spark
```

3. Run data pipeline:
```bash
python src/ingestion/batch/main.py
```

## Data Quality

We use Great Expectations for data quality validation. Run checks:
```bash
python -m great_expectations checkpoint run my_checkpoint
```

## Deployment

See `infrastructure/` for deployment configurations.

## License

Proprietary - Dulux Tech
