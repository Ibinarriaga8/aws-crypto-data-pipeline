"""Lambda function – Kinesis stream processor.

Triggered by a Kinesis Data Stream event source mapping.  Decodes each
record, applies real-time transformations, writes to S3 (processed zone)
and updates a DynamoDB table for low-latency API serving.

Environment variables
---------------------
PROCESSED_BUCKET    S3 bucket name for the processed data zone.
DYNAMODB_TABLE      DynamoDB table name for real-time price serving.
AWS_REGION          AWS region (injected by the Lambda runtime).
"""

from __future__ import annotations

import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "crypto-pipeline-processed")
_DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "crypto-prices-live")
_REGION = os.environ.get("AWS_REGION", "us-east-1")


def handler(event: dict, context) -> dict:
    """Lambda entry point for Kinesis event source mapping.

    Parameters
    ----------
    event:
        Kinesis event with a ``"Records"`` list.
    context:
        Lambda context object (unused).

    Returns
    -------
    dict
        HTTP-style response with a ``statusCode`` and processing summary.
    """
    from src.processing.stream_processor import StreamProcessor

    s3_client = boto3.client("s3", region_name=_REGION)
    dynamodb_client = boto3.client("dynamodb", region_name=_REGION)

    processor = StreamProcessor(
        s3_client=s3_client,
        dynamodb_client=dynamodb_client,
        processed_bucket=_PROCESSED_BUCKET,
        dynamodb_table=_DYNAMODB_TABLE,
    )

    result = processor.process_event(event)
    logger.info("Stream processor Lambda complete: %s", result)
    return {"statusCode": 200, "body": result}
