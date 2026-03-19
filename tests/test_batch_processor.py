"""Unit tests for the batch ETL processor."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.processing.batch_processor import (
    BatchProcessor,
    cleanse,
    enrich,
    parse_raw_records,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "symbol": "bitcoin",
        "name": "Bitcoin",
        "price_usd": 50000.0,
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
    },
    {
        "symbol": "ethereum",
        "name": "Ethereum",
        "price_usd": 3000.0,
        "market_cap_usd": 360_000_000_000.0,
        "volume_24h_usd": 15_000_000_000.0,
        "price_change_24h": -50.0,
        "price_change_pct_24h": -1.63,
        "circulating_supply": 120_000_000.0,
        "total_supply": None,
        "ath": 4868.0,
        "ath_change_pct": -38.4,
        "timestamp": "2024-01-01T00:00:00+00:00",
        "ingestion_timestamp": "2024-01-01T00:01:00+00:00",
        "source": "coingecko",
    },
]


# ---------------------------------------------------------------------------
# parse_raw_records
# ---------------------------------------------------------------------------


def test_parse_raw_records_from_list_of_dicts():
    df = parse_raw_records(SAMPLE_RECORDS)
    assert len(df) == 2
    assert "symbol" in df.columns


def test_parse_raw_records_from_nested_list():
    df = parse_raw_records([SAMPLE_RECORDS])
    assert len(df) == 2


def test_parse_raw_records_empty():
    df = parse_raw_records([])
    assert df.empty


# ---------------------------------------------------------------------------
# cleanse
# ---------------------------------------------------------------------------


def test_cleanse_drops_zero_price():
    records = [
        {**SAMPLE_RECORDS[0]},
        {**SAMPLE_RECORDS[0], "symbol": "bad_coin", "price_usd": 0},
    ]
    df = pd.DataFrame(records)
    cleaned = cleanse(df)
    assert len(cleaned) == 1
    assert "bad_coin" not in cleaned["symbol"].values


def test_cleanse_drops_null_symbol():
    records = [{**SAMPLE_RECORDS[0]}, {**SAMPLE_RECORDS[0], "symbol": None}]
    df = pd.DataFrame(records)
    cleaned = cleanse(df)
    assert len(cleaned) == 1


def test_cleanse_parses_timestamps():
    df = pd.DataFrame(SAMPLE_RECORDS)
    cleaned = cleanse(df)
    assert pd.api.types.is_datetime64_any_dtype(cleaned["timestamp"])
    assert pd.api.types.is_datetime64_any_dtype(cleaned["ingestion_timestamp"])


def test_cleanse_deduplicates_by_symbol_timestamp():
    # Two records for bitcoin at the same timestamp – keep latest ingestion
    record_old = {**SAMPLE_RECORDS[0], "ingestion_timestamp": "2024-01-01T00:00:00+00:00"}
    record_new = {**SAMPLE_RECORDS[0], "ingestion_timestamp": "2024-01-01T00:01:00+00:00"}
    df = pd.DataFrame([record_old, record_new])
    cleaned = cleanse(df)
    assert len(cleaned) == 1


def test_cleanse_empty_dataframe():
    cleaned = cleanse(pd.DataFrame())
    assert cleaned.empty


# ---------------------------------------------------------------------------
# enrich
# ---------------------------------------------------------------------------


def test_enrich_adds_required_columns():
    df = cleanse(pd.DataFrame(SAMPLE_RECORDS))
    enriched = enrich(df)
    assert "market_cap_rank" in enriched.columns
    assert "price_usd_log" in enriched.columns
    assert "processing_timestamp" in enriched.columns


def test_enrich_market_cap_rank_correct_order():
    df = cleanse(pd.DataFrame(SAMPLE_RECORDS))
    enriched = enrich(df)
    # Bitcoin has higher market cap; should rank 1
    btc_rank = enriched.loc[enriched["symbol"] == "bitcoin", "market_cap_rank"].values[0]
    eth_rank = enriched.loc[enriched["symbol"] == "ethereum", "market_cap_rank"].values[0]
    assert btc_rank < eth_rank


def test_enrich_price_usd_log_positive():
    df = cleanse(pd.DataFrame(SAMPLE_RECORDS))
    enriched = enrich(df)
    assert (enriched["price_usd_log"] > 0).all()


def test_enrich_empty_dataframe():
    enriched = enrich(pd.DataFrame())
    assert enriched.empty


# ---------------------------------------------------------------------------
# BatchProcessor (integration with mocked S3Handler)
# ---------------------------------------------------------------------------


class TestBatchProcessor:
    def _make_s3_handler(self, records=None):
        handler = MagicMock()
        handler.raw_bucket = "raw-bucket"
        handler.processed_bucket = "processed-bucket"
        handler.list_objects.return_value = ["raw/prices/btc.json"]
        handler.get_json.return_value = records or SAMPLE_RECORDS[0]
        handler.write_processed.return_value = "processed/prices/prices_20240101.parquet"
        handler.get_parquet.return_value = pd.DataFrame(SAMPLE_RECORDS)
        return handler

    def test_process_raw_prefix_returns_processed_key(self):
        handler = self._make_s3_handler()
        processor = BatchProcessor(handler, {})
        key = processor.process_raw_prefix("raw/prices/")
        assert key is not None
        assert "processed" in key

    def test_process_raw_prefix_no_objects_returns_none(self):
        handler = self._make_s3_handler()
        handler.list_objects.return_value = []
        processor = BatchProcessor(handler, {})
        key = processor.process_raw_prefix("raw/prices/")
        assert key is None

    def test_run_returns_row_count(self):
        handler = self._make_s3_handler()
        processor = BatchProcessor(handler, {})
        result = processor.run()
        assert result["row_count"] == len(SAMPLE_RECORDS)
        assert result["processed_key"] is not None
