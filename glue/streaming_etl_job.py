"""AWS Glue Streaming ETL job – Real-time processing of Kinesis records.

Reads continuously from a Kinesis Data Stream using Glue's Spark
Structured Streaming support, applies transformations, and writes
micro-batches to the processed S3 zone in Parquet format.

Job parameters
--------------
--KINESIS_STREAM_NAME   Kinesis stream name (required).
--PROCESSED_BUCKET      S3 bucket for processed Parquet output (required).
--AWS_REGION            AWS region of the Kinesis stream (required).
--CHECKPOINT_LOCATION   S3 path for Spark streaming checkpoint state
                        (required).
--JOB_NAME              Glue job name (injected automatically by Glue).
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Glue boilerplate
# ---------------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "KINESIS_STREAM_NAME",
        "PROCESSED_BUCKET",
        "AWS_REGION",
        "CHECKPOINT_LOCATION",
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

STREAM_NAME = args["KINESIS_STREAM_NAME"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
REGION = args["AWS_REGION"]
CHECKPOINT_LOCATION = args["CHECKPOINT_LOCATION"]

PROCESSED_PATH = f"s3://{PROCESSED_BUCKET}/processed/stream/"

# ---------------------------------------------------------------------------
# Schema for Kinesis payload
# ---------------------------------------------------------------------------

RECORD_SCHEMA = StructType(
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
# Read from Kinesis using Glue DataSource API
# ---------------------------------------------------------------------------

logger.info(f"Starting streaming job reading from Kinesis stream: {STREAM_NAME}")

kinesis_df = glueContext.create_data_frame_from_options(
    connection_type="kinesis",
    connection_options={
        "streamARN": f"arn:aws:kinesis:{REGION}:*:stream/{STREAM_NAME}",
        "startingPosition": "TRIM_HORIZON",
        "inferSchema": "false",
    },
    transformation_ctx="kinesis_source",
)

# ---------------------------------------------------------------------------
# Parse and transform
# ---------------------------------------------------------------------------

parsed_df = kinesis_df.select(
    F.from_json(F.col("data").cast("string"), RECORD_SCHEMA).alias("record")
).select("record.*")

transformed_df = (
    parsed_df
    .filter(F.col("symbol").isNotNull() & (F.col("price_usd") > 0))
    .withColumn("price_usd", F.round(F.col("price_usd"), 8))
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("stream_processing_timestamp", F.current_timestamp())
    .withColumn("year", F.year("timestamp"))
    .withColumn("month", F.month("timestamp"))
    .withColumn("day", F.dayofmonth("timestamp"))
)

# ---------------------------------------------------------------------------
# Write micro-batches to S3
# ---------------------------------------------------------------------------

query = (
    glueContext.write_data_frame_from_options(
        frame=transformed_df,
        connection_type="s3",
        connection_options={
            "path": PROCESSED_PATH,
            "partitionKeys": ["year", "month", "day"],
            "checkpointLocation": CHECKPOINT_LOCATION,
        },
        format="parquet",
        transformation_ctx="s3_sink",
    )
)

job.commit()
logger.info("Streaming ETL job running.")
