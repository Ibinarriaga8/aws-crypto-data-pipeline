"""AWS Glue PySpark ETL job – Batch processing of cryptocurrency price data.

This script is submitted to an AWS Glue job and reads raw JSON price records
from the raw S3 zone, applies schema enforcement and enrichment, then writes
columnar Parquet files to the processed and curated S3 zones.

Job parameters (passed via ``--`` prefix in the Glue job definition)
---------------------------------------------------------------------
--RAW_BUCKET            S3 bucket containing raw data (required).
--PROCESSED_BUCKET      S3 bucket for processed Parquet output (required).
--CURATED_BUCKET        S3 bucket for curated aggregate output (required).
--RAW_PREFIX            S3 key prefix for raw price records
                        (default ``raw/prices/``).
--GLUE_DATABASE         Glue Data Catalog database name
                        (default ``crypto_pipeline_db``).
--PROCESSED_TABLE       Glue table name for processed data
                        (default ``crypto_prices_processed``).
--JOB_NAME              Glue job name (injected automatically by Glue).
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Glue boilerplate
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_BUCKET",
        "PROCESSED_BUCKET",
        "CURATED_BUCKET",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

RAW_BUCKET = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
CURATED_BUCKET = args["CURATED_BUCKET"]
RAW_PREFIX = args.get("RAW_PREFIX", "raw/prices/")
GLUE_DATABASE = args.get("GLUE_DATABASE", "crypto_pipeline_db")
PROCESSED_TABLE = args.get("PROCESSED_TABLE", "crypto_prices_processed")

RAW_PATH = f"s3://{RAW_BUCKET}/{RAW_PREFIX}"
PROCESSED_PATH = f"s3://{PROCESSED_BUCKET}/processed/prices/"
CURATED_PATH = f"s3://{CURATED_BUCKET}/curated/prices_aggregated/"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

PRICE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("market_cap_usd", DoubleType(), True),
        StructField("volume_24h_usd", DoubleType(), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_pct_24h", DoubleType(), True),
        StructField("circulating_supply", DoubleType(), True),
        StructField("total_supply", DoubleType(), True),
        StructField("ath", DoubleType(), True),
        StructField("ath_change_pct", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------


def read_raw(path: str) -> DataFrame:
    """Read raw JSON files from S3 and apply the pipeline schema."""
    logger.info(f"Reading raw data from {path}")
    return (
        spark.read.schema(PRICE_SCHEMA)
        .option("multiLine", "true")
        .json(path)
    )


def cleanse(df: DataFrame) -> DataFrame:
    """Drop null symbols/prices, parse timestamps, deduplicate."""
    df = df.filter(F.col("symbol").isNotNull() & (F.col("price_usd") > 0))
    df = df.withColumn("timestamp", F.to_timestamp("timestamp")) \
           .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    # Keep the latest ingestion per (symbol, timestamp)
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("symbol", "timestamp")
        .orderBy(F.col("ingestion_timestamp").desc())
    )
    df = (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )
    return df


def enrich(df: DataFrame) -> DataFrame:
    """Add derived analytic columns."""
    df = df.withColumn("price_usd_log", F.log(F.col("price_usd")))
    df = df.withColumn(
        "market_cap_rank",
        F.rank().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.orderBy(F.col("market_cap_usd").desc())
        ),
    )
    df = df.withColumn(
        "processing_timestamp",
        F.current_timestamp(),
    )
    df = df.withColumn(
        "year", F.year("timestamp")
    ).withColumn(
        "month", F.month("timestamp")
    ).withColumn(
        "day", F.dayofmonth("timestamp")
    )
    return df


def aggregate(df: DataFrame) -> DataFrame:
    """Compute daily aggregates per symbol for the curated zone."""
    return (
        df.groupBy(F.col("symbol"), F.col("year"), F.col("month"), F.col("day"))
        .agg(
            F.min("price_usd").alias("price_low"),
            F.max("price_usd").alias("price_high"),
            F.avg("price_usd").alias("price_avg"),
            F.last("price_usd").alias("price_close"),
            F.sum("volume_24h_usd").alias("total_volume"),
            F.last("market_cap_usd").alias("market_cap_usd"),
            F.last("market_cap_rank").alias("market_cap_rank"),
            F.count("*").alias("record_count"),
        )
        .orderBy("symbol", "year", "month", "day")
    )


# ---------------------------------------------------------------------------
# Main ETL flow
# ---------------------------------------------------------------------------

logger.info("Starting Glue batch ETL job.")

raw_df = read_raw(RAW_PATH)
logger.info(f"Raw record count: {raw_df.count()}")

cleansed_df = cleanse(raw_df)
enriched_df = enrich(cleansed_df)
logger.info(f"Processed record count: {enriched_df.count()}")

# Write processed Parquet (partitioned by year/month/day)
enriched_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
    PROCESSED_PATH
)
logger.info(f"Wrote processed data to {PROCESSED_PATH}")

# Compute and write daily aggregates to the curated zone
curated_df = aggregate(enriched_df)
curated_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
    CURATED_PATH
)
logger.info(f"Wrote curated data to {CURATED_PATH}")

job.commit()
logger.info("Glue ETL job complete.")
