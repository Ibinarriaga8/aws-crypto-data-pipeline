"""Real-time stream processor.

Consumes records from an Amazon Kinesis Data Stream, applies lightweight
transformations, and writes results to both the processed S3 zone and a
DynamoDB table for low-latency serving.
"""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def decode_kinesis_record(kinesis_record: dict) -> dict | None:
    """Decode a single Kinesis record payload.

    Parameters
    ----------
    kinesis_record:
        A record element from a Kinesis ``GetRecords`` or Lambda event, which
        may carry its ``Data`` field either as raw bytes or as a base-64
        encoded string (as delivered by Lambda event source mappings).

    Returns
    -------
    dict or None
        Deserialised payload, or ``None`` if decoding fails.
    """
    try:
        data = kinesis_record.get("kinesis", {}).get("data") or kinesis_record.get("Data")
        if isinstance(data, bytes):
            payload = data
        else:
            payload = base64.b64decode(data)
        return json.loads(payload.decode("utf-8"))
    except Exception:
        logger.exception("Failed to decode Kinesis record.")
        return None


def transform_stream_record(record: dict) -> dict:
    """Apply lightweight transformations to a streaming price record.

    Adds a ``stream_processing_timestamp`` field and rounds ``price_usd``
    to 8 decimal places for consistency with exchange tick sizes.

    Parameters
    ----------
    record:
        Decoded price record from the Kinesis stream.

    Returns
    -------
    dict
        Transformed record.
    """
    record = dict(record)
    record["stream_processing_timestamp"] = datetime.now(timezone.utc).isoformat()
    if "price_usd" in record and record["price_usd"] is not None:
        record["price_usd"] = round(float(record["price_usd"]), 8)
    return record


class StreamProcessor:
    """Processes Kinesis records and persists results to S3 and DynamoDB.

    Parameters
    ----------
    s3_client:
        A ``boto3`` S3 client.
    dynamodb_client:
        A ``boto3`` DynamoDB client.
    processed_bucket:
        Name of the S3 processed-zone bucket.
    dynamodb_table:
        Name of the DynamoDB table used for real-time serving.
    """

    def __init__(
        self,
        s3_client,
        dynamodb_client,
        processed_bucket: str,
        dynamodb_table: str,
    ) -> None:
        self.s3_client = s3_client
        self.dynamodb_client = dynamodb_client
        self.processed_bucket = processed_bucket
        self.dynamodb_table = dynamodb_table

    def _s3_key(self, symbol: str, ts: datetime) -> str:
        return (
            f"processed/stream/year={ts.year}/month={ts.month:02d}/"
            f"day={ts.day:02d}/{symbol}_{ts.strftime('%Y%m%dT%H%M%S%fZ')}.json"
        )

    def _to_dynamodb_item(self, record: dict) -> dict:
        """Convert a price record to a DynamoDB item (all values as strings)."""
        ts = record.get("timestamp") or datetime.now(timezone.utc).isoformat()
        return {
            "symbol": {"S": str(record.get("symbol", ""))},
            "timestamp": {"S": str(ts)},
            "price_usd": {"N": str(record.get("price_usd", 0))},
            "market_cap_usd": {"N": str(record.get("market_cap_usd", 0))},
            "volume_24h_usd": {"N": str(record.get("volume_24h_usd", 0))},
            "price_change_pct_24h": {"N": str(record.get("price_change_pct_24h", 0))},
            "stream_processing_timestamp": {
                "S": str(record.get("stream_processing_timestamp", ""))
            },
            "source": {"S": str(record.get("source", ""))},
        }

    def process_record(self, record: dict) -> bool:
        """Transform a single decoded record and persist to S3 and DynamoDB.

        Parameters
        ----------
        record:
            Decoded price record.

        Returns
        -------
        bool
            ``True`` on success, ``False`` on failure.
        """
        try:
            transformed = transform_stream_record(record)

            # Write to S3
            ts = datetime.now(timezone.utc)
            key = self._s3_key(transformed.get("symbol", "unknown"), ts)
            self.s3_client.put_object(
                Bucket=self.processed_bucket,
                Key=key,
                Body=json.dumps(transformed).encode("utf-8"),
                ContentType="application/json",
            )

            # Write to DynamoDB for low-latency serving
            item = self._to_dynamodb_item(transformed)
            self.dynamodb_client.put_item(
                TableName=self.dynamodb_table,
                Item=item,
            )
            logger.debug("Processed record for symbol=%s", transformed.get("symbol"))
            return True
        except Exception:
            logger.exception("Failed to process record: %s", record)
            return False

    def process_event(self, event: dict) -> dict:
        """Entry point for a Lambda ``event`` from a Kinesis event source mapping.

        Parameters
        ----------
        event:
            Lambda event dict containing a ``"Records"`` list.

        Returns
        -------
        dict
            ``{"success": int, "failed": int}``
        """
        records = event.get("Records", [])
        success = 0
        failed = 0
        for kinesis_record in records:
            decoded = decode_kinesis_record(kinesis_record)
            if decoded is None:
                failed += 1
                continue
            if self.process_record(decoded):
                success += 1
            else:
                failed += 1

        logger.info("Stream processing complete: success=%d, failed=%d", success, failed)
        return {"success": success, "failed": failed}
