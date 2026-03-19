"""Unit tests for the S3 storage handler."""

from __future__ import annotations

import io
import json

import boto3
import pandas as pd
import pytest
from moto import mock_aws


REGION = "us-east-1"
RAW_BUCKET = "test-raw"
PROCESSED_BUCKET = "test-processed"
CURATED_BUCKET = "test-curated"


@pytest.fixture
def s3_setup():
    """Create mock S3 buckets and return a boto3 client."""
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        for bucket in (RAW_BUCKET, PROCESSED_BUCKET, CURATED_BUCKET):
            client.create_bucket(Bucket=bucket)
        yield client


@pytest.fixture
def handler(s3_setup):
    from src.storage.s3_handler import S3Handler

    return S3Handler(
        s3_client=s3_setup,
        raw_bucket=RAW_BUCKET,
        processed_bucket=PROCESSED_BUCKET,
        curated_bucket=CURATED_BUCKET,
    )


# ---------------------------------------------------------------------------
# put_json / get_json round-trip
# ---------------------------------------------------------------------------


def test_put_and_get_json(handler, s3_setup):
    data = {"symbol": "bitcoin", "price_usd": 50000.0}
    handler.put_json(RAW_BUCKET, "test/record.json", data)

    result = handler.get_json(RAW_BUCKET, "test/record.json")
    assert result == data


def test_get_json_missing_key_raises(handler):
    with pytest.raises(Exception):
        handler.get_json(RAW_BUCKET, "nonexistent/key.json")


# ---------------------------------------------------------------------------
# put_parquet / get_parquet round-trip
# ---------------------------------------------------------------------------


def test_put_and_get_parquet(handler, s3_setup):
    df = pd.DataFrame(
        {
            "symbol": ["bitcoin", "ethereum"],
            "price_usd": [50000.0, 3000.0],
        }
    )
    handler.put_parquet(PROCESSED_BUCKET, "test/prices.parquet", df)

    result = handler.get_parquet(PROCESSED_BUCKET, "test/prices.parquet")
    assert list(result.columns) == ["symbol", "price_usd"]
    assert result["symbol"].tolist() == ["bitcoin", "ethereum"]


# ---------------------------------------------------------------------------
# list_objects
# ---------------------------------------------------------------------------


def test_list_objects_returns_all_keys(handler, s3_setup):
    for i in range(3):
        handler.put_json(RAW_BUCKET, f"prefix/file_{i}.json", {"i": i})

    keys = handler.list_objects(RAW_BUCKET, prefix="prefix/")
    assert len(keys) == 3
    assert all(k.startswith("prefix/") for k in keys)


def test_list_objects_empty_prefix(handler, s3_setup):
    keys = handler.list_objects(RAW_BUCKET, prefix="nonexistent/")
    assert keys == []


# ---------------------------------------------------------------------------
# write_raw / write_processed / write_curated
# ---------------------------------------------------------------------------


def test_write_raw_creates_partitioned_key(handler, s3_setup):
    key = handler.write_raw("prices", "btc.json", {"symbol": "bitcoin"})

    assert "raw/prices/" in key
    assert "btc.json" in key
    # Verify the object exists
    keys = handler.list_objects(RAW_BUCKET, prefix="raw/prices/")
    assert key in keys


def test_write_processed_creates_parquet(handler, s3_setup):
    df = pd.DataFrame({"symbol": ["bitcoin"], "price_usd": [50000.0]})
    key = handler.write_processed("prices", "prices.parquet", df)

    assert "processed/prices/" in key
    keys = handler.list_objects(PROCESSED_BUCKET, prefix="processed/prices/")
    assert key in keys


def test_write_curated_creates_parquet(handler, s3_setup):
    df = pd.DataFrame({"symbol": ["bitcoin"], "price_avg": [49000.0]})
    key = handler.write_curated("prices_aggregated", "agg.parquet", df)

    assert "curated/prices_aggregated/" in key
    keys = handler.list_objects(CURATED_BUCKET, prefix="curated/prices_aggregated/")
    assert key in keys


# ---------------------------------------------------------------------------
# move_to_processed
# ---------------------------------------------------------------------------


def test_move_to_processed_copies_object(handler, s3_setup):
    handler.put_json(RAW_BUCKET, "raw/prices/btc.json", {"symbol": "bitcoin"})
    handler.move_to_processed("raw/prices/btc.json", "processed/prices/btc.json")

    # The copy should appear in the processed bucket
    keys = handler.list_objects(PROCESSED_BUCKET, prefix="processed/")
    assert "processed/prices/btc.json" in keys
