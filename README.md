# AWS Crypto Data Pipeline

## Executive Summary
This project delivers an end-to-end cloud-based data pipeline on AWS for processing and analyzing cryptocurrency market data. 
The solution integrates batch and real-time processing, enabling scalable analytics and actionable insights.

## Business Context
Financial markets require timely and reliable data processing to support decision-making. 
This project addresses the need for:
- Real-time monitoring of crypto assets
- Historical trend analysis
- Scalable data infrastructure for analytics and ML

## Solution Overview
The architecture follows a modern Data Lake approach:

- Ingestion: Amazon S3 stores raw data
- Processing: AWS Glue (Spark) performs ETL transformations
- Storage: Structured datasets stored in S3 (Parquet format)
- Query: Amazon Athena enables serverless querying
- Visualization: Amazon QuickSight dashboards
- Streaming: Kafka + Grafana for real-time insights

## Data Architecture
The pipeline follows a Medallion Architecture:
- Bronze: Raw ingestion layer
- Silver: Cleaned and enriched data
- Gold: Aggregated KPIs and business metrics

## Key Features
- Scalable cloud-native architecture
- Batch + streaming integration
- Serverless analytics (Athena)
- Near real-time monitoring
- Financial indicators (SMA, EMA, candlestick analysis)

## Technology Stack
- AWS (S3, Glue, Athena, QuickSight)
- Apache Spark
- Apache Kafka
- Grafana
- Python

## Repository Structure
- data/ → Sample datasets
- etl/ → Glue ETL jobs
- streaming/ → Kafka producer & consumer
- infrastructure/ → IaC (Terraform/CloudFormation)
- dashboards/ → Visualization layer
- notebooks/ → Exploratory analysis

## How to Run
```bash
pip install -r requirements.txt
python streaming/kafka_producer.py
python streaming/kafka_consumer.py
```

## Results & Insights
- BTC and ETH trend analysis
- Moving averages (SMA, EMA)
- Comparative technical indicators
- Real-time dashboards

## Business Impact
- Enables data-driven trading insights
- Reduces time-to-insight via serverless analytics
- Scalable solution adaptable to other financial datasets

## Future Enhancements
- Machine Learning models for trading signals
- Real-time anomaly detection
- Expansion to multi-asset portfolios

## Authors
- Jorge Ibinarriaga Robles
- Bernardo Gómez Carrasco
- Enrique Rodríguez Camacho
