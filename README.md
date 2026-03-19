# aws-crypto-data-pipeline

End-to-end cloud-based data pipeline on AWS for processing and analysing
cryptocurrency market data, integrating batch and real-time processing to
enable scalable analytics and actionable insights.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          AWS Crypto Data Pipeline                          │
│                                                                            │
│  ┌──────────────────┐   schedule    ┌─────────────────────────────────┐   │
│  │  EventBridge     │──────────────▶│  Lambda: crypto-data-ingestion  │   │
│  │  (every minute)  │               │  (batch + real-time producer)   │   │
│  └──────────────────┘               └────────┬──────────────┬─────────┘   │
│                                              │              │             │
│                              S3 raw zone     │              │ Kinesis     │
│                         ┌────────────────────▼──┐     ┌─────▼──────────┐ │
│                         │  S3: crypto-raw        │     │ Kinesis Data   │ │
│                         │  (JSON, partitioned    │     │ Stream         │ │
│                         │   by year/month/day)   │     │ (shards x 2)  │ │
│                         └───────────┬────────────┘     └──────┬─────────┘ │
│                                     │                         │           │
│  ┌──────────────────────────────────▼─────┐    ┌─────────────▼─────────┐ │
│  │  AWS Glue: batch_etl_job               │    │ Lambda: stream-       │ │
│  │  (cleanse -> enrich -> Parquet)        │    │ processor             │ │
│  └─────────────────────┬──────────────────┘    └───────┬───────────────┘ │
│                         │                              │                  │
│              ┌──────────▼───────────┐    ┌────────────▼────────────────┐ │
│              │  S3: crypto-         │    │  S3: processed/stream/      │ │
│              │  processed           │    │  DynamoDB: crypto-prices-   │ │
│              │  (Parquet)           │    │  live (low-latency serving) │ │
│              └──────────┬───────────┘    └─────────────────────────────┘ │
│                         │                                                 │
│  ┌──────────────────────▼──────────────────────────────────────────────┐ │
│  │  AWS Glue: batch_etl_job  ->  S3: crypto-curated (daily aggregates) │ │
│  └──────────────────────┬────────────────────────────────────────────── ┘ │
│                         │                                                  │
│              ┌──────────▼──────────────────────────┐                      │
│              │  Amazon Athena + Glue Data Catalog   │                      │
│              │  (SQL analytics on processed data)   │                      │
│              └─────────────────────────────────────┘                      │
│                                                                            │
│  CloudWatch Metrics / Alarms  -->  SNS Alert Topic                        │
└────────────────────────────────────────────────────────────────────────────┘
```

### Data Zones (S3 Data Lake)

| Zone | Bucket suffix | Format | Contents |
|------|---------------|--------|----------|
| Raw | `-raw-...` | JSON | Original API payloads, partitioned by `year/month/day` |
| Processed | `-processed-...` | Parquet | Cleansed, enriched, deduplicated records |
| Curated | `-curated-...` | Parquet | Daily OHLCV-style aggregates per coin |

---

## Repository Layout

```
aws-crypto-data-pipeline/
├── config/
│   ├── config.yaml              # Pipeline configuration
│   └── schema/
│       └── crypto_schema.json   # JSON Schema for price records
├── src/
│   ├── ingestion/
│   │   ├── batch_ingestion.py   # CoinGecko API client + S3 batch writer
│   │   └── realtime_ingestion.py# Kinesis producer (polling loop)
│   ├── processing/
│   │   ├── batch_processor.py   # Cleanse / enrich / write Parquet
│   │   └── stream_processor.py  # Kinesis record decoder / DynamoDB writer
│   ├── storage/
│   │   └── s3_handler.py        # S3 data lake helper
│   └── analytics/
│       └── athena_queries.py    # Athena runner + pre-built analytics queries
├── lambda/
│   ├── crypto_ingestion/
│   │   └── lambda_function.py   # Ingestion Lambda entry point
│   └── stream_processor/
│       └── lambda_function.py   # Stream processor Lambda entry point
├── glue/
│   ├── batch_etl_job.py         # Glue PySpark batch ETL script
│   └── streaming_etl_job.py     # Glue PySpark streaming ETL script
├── infrastructure/
│   ├── deploy.sh                # One-command deployment script
│   └── cloudformation/
│       ├── main.yaml            # Root / nested-stack orchestrator
│       ├── s3.yaml              # S3 buckets
│       ├── kinesis.yaml         # Kinesis stream + CloudWatch alarm
│       ├── lambda.yaml          # Lambda functions + EventBridge rule
│       └── glue.yaml            # Glue DB, crawlers, jobs, Athena workgroup
├── tests/                       # pytest unit tests (70 tests)
├── requirements.txt
└── README.md
```

---

## Prerequisites

- Python >= 3.12
- AWS CLI configured with appropriate IAM permissions
- An S3 bucket to host CloudFormation templates and Lambda ZIP packages

---

## Local Development

```bash
# 1. Create and activate a virtual environment
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the full test suite
pytest tests/ -v
```

---

## Deployment

```bash
export TEMPLATES_BUCKET=my-cf-templates-bucket
export ALERT_EMAIL=ops@example.com   # optional

./infrastructure/deploy.sh --env dev --region us-east-1
```

The script will:
1. Sync CloudFormation templates to S3.
2. Upload the Glue PySpark scripts to S3.
3. Package each Lambda function (with its `src/` dependencies) into a ZIP and upload it.
4. Deploy the root CloudFormation stack (which in turn deploys all nested stacks).

---

## Configuration

Edit `config/config.yaml` to change:

- **Coin list** (`crypto.symbols`) – any CoinGecko coin IDs.
- **Ingestion frequency** (`crypto.fetch_interval_seconds`).
- **Historical window** (`crypto.historical_days`).
- **Kinesis shard count**, S3 bucket names, Glue worker types, etc.

---

## Data Flow

### Batch Path (hourly / on-demand)

```
EventBridge -> Lambda (ingestion)
           -> CoinGecko API -> S3 raw (JSON)
           -> Glue batch ETL -> S3 processed (Parquet) -> S3 curated (aggregates)
           -> Athena / Glue Catalog for SQL analytics
```

### Real-time Path (every minute)

```
EventBridge -> Lambda (ingestion)
           -> CoinGecko API -> Kinesis Data Stream
           -> Lambda (stream processor)
           -> S3 processed/stream (JSON)
           -> DynamoDB crypto-prices-live (low-latency reads)
```

---

## Analytics Queries

Use `CryptoAnalytics` in `src/analytics/athena_queries.py` or query Athena directly:

```sql
-- Top 10 coins by market cap
SELECT symbol, name, price_usd, market_cap_usd
FROM crypto_prices_processed
ORDER BY market_cap_usd DESC
LIMIT 10;

-- 24-hour market movers
SELECT symbol, name, price_change_pct_24h
FROM crypto_prices_processed
ORDER BY price_change_pct_24h DESC;

-- Market dominance
SELECT symbol, name,
       ROUND(100.0 * market_cap_usd / SUM(market_cap_usd) OVER (), 2) AS dominance_pct
FROM crypto_prices_processed
ORDER BY dominance_pct DESC;
```

---

## Security

- All S3 buckets have **public access blocked** and **AES-256 encryption** enabled.
- The Kinesis stream uses **KMS encryption** (`alias/aws/kinesis`).
- Lambda and Glue IAM roles follow **least-privilege** principles.
- Secrets (API keys, alert email) are passed via environment variables or AWS
  Parameter Store – never committed to source code.

---

## License

[MIT](LICENSE)
