"""Lambda function – Cryptocurrency data ingestion.

This function is triggered on a schedule (EventBridge) and ingests both
current price data and historical data for all configured coins.

Environment variables
---------------------
RAW_BUCKET          S3 bucket name for the raw data zone.
KINESIS_STREAM_NAME Kinesis stream name for real-time records.
CONFIG_BUCKET       S3 bucket where ``config/config.yaml`` is stored
                    (optional; falls back to the bundled file).
AWS_REGION          AWS region (injected by the Lambda runtime).
"""

from __future__ import annotations

import json
import logging
import os
import sys

import boto3
import yaml

# Allow imports from the Lambda deployment package root
sys.path.insert(0, os.path.dirname(__file__))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_RAW_BUCKET = os.environ.get("RAW_BUCKET", "crypto-pipeline-raw")
_KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME", "crypto-price-stream")
_REGION = os.environ.get("AWS_REGION", "us-east-1")

_DEFAULT_CONFIG = {
    "crypto": {
        "api_base_url": "https://api.coingecko.com/api/v3",
        "symbols": [
            "bitcoin",
            "ethereum",
            "binancecoin",
            "solana",
            "cardano",
            "ripple",
            "polkadot",
            "dogecoin",
        ],
        "vs_currency": "usd",
        "historical_days": 365,
    },
    "kinesis": {"fetch_interval_seconds": 60},
}


def _load_config() -> dict:
    """Load config from a bundled YAML file or return defaults."""
    config_path = os.path.join(os.path.dirname(__file__), "config", "config.yaml")
    if os.path.exists(config_path):
        with open(config_path) as fh:
            return yaml.safe_load(fh)
    return _DEFAULT_CONFIG


def handler(event: dict, context) -> dict:
    """Lambda entry point.

    Parameters
    ----------
    event:
        EventBridge scheduled event or a manual invocation payload.
        Supports ``{"mode": "prices"}`` or ``{"mode": "historical"}`` to
        run only part of the ingestion.  Defaults to running both.
    context:
        Lambda context object (unused).

    Returns
    -------
    dict
        Summary of written S3 keys.
    """
    from src.ingestion.batch_ingestion import BatchIngestionJob
    from src.ingestion.realtime_ingestion import RealtimeIngestionJob

    config = _load_config()

    s3_client = boto3.client("s3", region_name=_REGION)
    kinesis_client = boto3.client("kinesis", region_name=_REGION)

    mode = event.get("mode", "both")
    result: dict = {}

    if mode in ("prices", "both"):
        batch_job = BatchIngestionJob(
            s3_client=s3_client,
            raw_bucket=_RAW_BUCKET,
            config=config,
        )
        batch_result = batch_job.ingest_current_prices()
        result["price_keys"] = batch_result

        # Also publish to Kinesis for real-time consumers
        rt_job = RealtimeIngestionJob(
            kinesis_client=kinesis_client,
            stream_name=_KINESIS_STREAM,
            config=config,
        )
        rt_job.fetch_and_publish()

    if mode in ("historical", "both"):
        batch_job = BatchIngestionJob(
            s3_client=s3_client,
            raw_bucket=_RAW_BUCKET,
            config=config,
        )
        history_keys: list[str] = []
        for coin_id in config.get("crypto", {}).get("symbols", []):
            try:
                key = batch_job.ingest_historical_prices(coin_id)
                history_keys.append(key)
            except Exception:
                logger.exception("Failed to ingest history for %s", coin_id)
        result["history_keys"] = history_keys

    logger.info("Ingestion Lambda complete: %s", json.dumps(result))
    return {"statusCode": 200, "body": json.dumps(result)}
