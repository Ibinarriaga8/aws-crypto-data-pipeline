"""Unit tests for the real-time stream processor."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.processing.stream_processor import (
    StreamProcessor,
    decode_kinesis_record,
    transform_stream_record,
)


# ---------------------------------------------------------------------------
# decode_kinesis_record
# ---------------------------------------------------------------------------

SAMPLE_RECORD = {
    "symbol": "bitcoin",
    "name": "Bitcoin",
    "price_usd": 50000.123456789,
    "market_cap_usd": 950_000_000_000.0,
    "volume_24h_usd": 30_000_000_000.0,
    "price_change_24h": 500.0,
    "price_change_pct_24h": 1.01,
    "circulating_supply": 19_000_000.0,
    "total_supply": 21_000_000.0,
    "ath": 69_000.0,
    "ath_change_pct": -27.5,
    "timestamp": "2024-01-01T00:00:00+00:00",
    "ingestion_timestamp": "2024-01-01T00:01:00+00:00",
    "source": "coingecko",
}


def _lambda_kinesis_record(payload: dict) -> dict:
    """Build a Lambda-style Kinesis event record."""
    return {
        "kinesis": {
            "data": base64.b64encode(json.dumps(payload).encode()).decode(),
        }
    }


def test_decode_kinesis_record_lambda_style():
    raw = _lambda_kinesis_record(SAMPLE_RECORD)
    decoded = decode_kinesis_record(raw)
    assert decoded == SAMPLE_RECORD


def test_decode_kinesis_record_raw_bytes():
    raw = {"Data": json.dumps(SAMPLE_RECORD).encode("utf-8")}
    decoded = decode_kinesis_record(raw)
    assert decoded == SAMPLE_RECORD


def test_decode_kinesis_record_invalid_returns_none():
    decoded = decode_kinesis_record({"kinesis": {"data": "!!!not-base64!!!"}})
    assert decoded is None


# ---------------------------------------------------------------------------
# transform_stream_record
# ---------------------------------------------------------------------------


def test_transform_adds_processing_timestamp():
    transformed = transform_stream_record(dict(SAMPLE_RECORD))
    assert "stream_processing_timestamp" in transformed


def test_transform_rounds_price():
    record = {**SAMPLE_RECORD, "price_usd": 50000.123456789012}
    transformed = transform_stream_record(record)
    # Should be rounded to 8 decimal places
    assert transformed["price_usd"] == round(50000.123456789012, 8)


def test_transform_does_not_mutate_input():
    original = dict(SAMPLE_RECORD)
    transform_stream_record(original)
    assert "stream_processing_timestamp" not in original


# ---------------------------------------------------------------------------
# StreamProcessor
# ---------------------------------------------------------------------------


class TestStreamProcessor:
    def _make_clients(self):
        s3 = MagicMock()
        s3.put_object.return_value = {}
        dynamodb = MagicMock()
        dynamodb.put_item.return_value = {}
        return s3, dynamodb

    def _make_processor(self, s3=None, dynamodb=None):
        s3 = s3 or MagicMock()
        dynamodb = dynamodb or MagicMock()
        return StreamProcessor(
            s3_client=s3,
            dynamodb_client=dynamodb,
            processed_bucket="processed-bucket",
            dynamodb_table="crypto-prices-live",
        )

    def test_process_record_writes_to_s3(self):
        s3, dynamodb = self._make_clients()
        proc = StreamProcessor(s3, dynamodb, "processed-bucket", "crypto-prices-live")
        proc.process_record(dict(SAMPLE_RECORD))
        s3.put_object.assert_called_once()

    def test_process_record_writes_to_dynamodb(self):
        s3, dynamodb = self._make_clients()
        proc = StreamProcessor(s3, dynamodb, "processed-bucket", "crypto-prices-live")
        proc.process_record(dict(SAMPLE_RECORD))
        dynamodb.put_item.assert_called_once()
        item = dynamodb.put_item.call_args[1]["Item"]
        assert item["symbol"]["S"] == "bitcoin"

    def test_process_record_returns_false_on_s3_error(self):
        s3, dynamodb = self._make_clients()
        s3.put_object.side_effect = RuntimeError("S3 error")
        proc = StreamProcessor(s3, dynamodb, "processed-bucket", "crypto-prices-live")
        result = proc.process_record(dict(SAMPLE_RECORD))
        assert result is False

    def test_process_event_counts_successes(self):
        s3, dynamodb = self._make_clients()
        proc = StreamProcessor(s3, dynamodb, "processed-bucket", "crypto-prices-live")
        event = {
            "Records": [
                _lambda_kinesis_record(SAMPLE_RECORD),
                _lambda_kinesis_record({**SAMPLE_RECORD, "symbol": "ethereum"}),
            ]
        }
        result = proc.process_event(event)
        assert result["success"] == 2
        assert result["failed"] == 0

    def test_process_event_counts_decode_failures(self):
        s3, dynamodb = self._make_clients()
        proc = StreamProcessor(s3, dynamodb, "processed-bucket", "crypto-prices-live")
        event = {
            "Records": [
                {"kinesis": {"data": "!!!invalid!!!"}},
            ]
        }
        result = proc.process_event(event)
        assert result["failed"] == 1
        assert result["success"] == 0

    def test_process_event_empty_records(self):
        proc = self._make_processor()
        result = proc.process_event({"Records": []})
        assert result == {"success": 0, "failed": 0}

    def test_to_dynamodb_item_has_required_fields(self):
        s3, dynamodb = self._make_clients()
        proc = StreamProcessor(s3, dynamodb, "processed-bucket", "crypto-prices-live")
        item = proc._to_dynamodb_item(SAMPLE_RECORD)
        assert "symbol" in item
        assert "timestamp" in item
        assert "price_usd" in item
        assert item["symbol"]["S"] == "bitcoin"
        assert "N" in item["price_usd"]
