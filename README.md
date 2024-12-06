# Customer Analytics Data Pipeline

## Project Overview
This project builds an end-to-end data pipeline to process customer activity data for analytics. The pipeline ingests real-time data from Kafka, processes it using Spark, and stores it in Redshift for analytical querying.

## Tools and Technologies
- **Kafka**: For real-time data ingestion.
- **S3**: Intermediate storage for raw and processed data.
- **Spark**: For large-scale data transformation.
- **Redshift**: For data warehousing.
- **Airflow**: For orchestration (optional).

## Directory Structure

```
├── airflow/
│   └── # Airflow DAGs and scripts (optional)
├── data/
│   └── # Local storage for testing data
├── kafka/
│   └── # Kafka producer and consumer scripts
├── redshift/
│   └── # Redshift schema and table definitions
├── spark/
│   └── # Spark ETL scripts
├── scripts/
│   └── # Utility scripts
├── README.md
│   └── # Project documentation
├── .gitignore
└── # Ignored files and folders
```
