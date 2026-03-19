"""Unit tests for the batch ingestion module."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.batch_ingestion import (
    BatchIngestionJob,
    CoinGeckoClient,
    normalise_market_record,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_MARKET_RECORD = {
    "id": "bitcoin",
    "name": "Bitcoin",
    "current_price": 50000.0,
    "market_cap": 950_000_000_000,
    "total_volume": 30_000_000_000,
    "price_change_24h": 500.0,
    "price_change_percentage_24h": 1.01,
    "circulating_supply": 19_000_000,
    "total_supply": 21_000_000,
    "ath": 69_000.0,
    "ath_change_percentage": -27.5,
    "last_updated": "2024-01-01T00:00:00Z",
}

SAMPLE_CONFIG = {
    "crypto": {
        "api_base_url": "https://api.coingecko.com/api/v3",
        "symbols": ["bitcoin", "ethereum"],
        "vs_currency": "usd",
        "historical_days": 30,
    },
    "kinesis": {},
}


# ---------------------------------------------------------------------------
# normalise_market_record
# ---------------------------------------------------------------------------


def test_normalise_market_record_fields():
    """All required schema fields should be present and correctly mapped."""
    ingestion_ts = "2024-01-01T00:00:00+00:00"
    result = normalise_market_record(SAMPLE_MARKET_RECORD, ingestion_ts)

    assert result["symbol"] == "bitcoin"
    assert result["name"] == "Bitcoin"
    assert result["price_usd"] == 50000.0
    assert result["market_cap_usd"] == 950_000_000_000
    assert result["volume_24h_usd"] == 30_000_000_000
    assert result["price_change_24h"] == 500.0
    assert result["price_change_pct_24h"] == 1.01
    assert result["circulating_supply"] == 19_000_000
    assert result["total_supply"] == 21_000_000
    assert result["ath"] == 69_000.0
    assert result["ath_change_pct"] == -27.5
    assert result["timestamp"] == "2024-01-01T00:00:00Z"
    assert result["ingestion_timestamp"] == ingestion_ts
    assert result["source"] == "coingecko"


def test_normalise_market_record_missing_price():
    """Missing price should default to 0.0."""
    record = dict(SAMPLE_MARKET_RECORD)
    record["current_price"] = None
    result = normalise_market_record(record, "2024-01-01T00:00:00+00:00")
    assert result["price_usd"] == 0.0


def test_normalise_market_record_uses_ingestion_ts_when_no_last_updated():
    """If ``last_updated`` is absent, ``timestamp`` should fall back to ingestion_ts."""
    record = {k: v for k, v in SAMPLE_MARKET_RECORD.items() if k != "last_updated"}
    ingestion_ts = "2024-06-01T12:00:00+00:00"
    result = normalise_market_record(record, ingestion_ts)
    assert result["timestamp"] == ingestion_ts


# ---------------------------------------------------------------------------
# CoinGeckoClient
# ---------------------------------------------------------------------------


class TestCoinGeckoClient:
    def test_get_markets_calls_correct_url(self):
        client = CoinGeckoClient(base_url="https://fake-api.test")
        mock_response = MagicMock()
        mock_response.json.return_value = [SAMPLE_MARKET_RECORD]
        mock_response.raise_for_status.return_value = None

        with patch.object(client.session, "get", return_value=mock_response) as mock_get:
            result = client.get_markets(vs_currency="usd", ids=["bitcoin"])

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert "coins/markets" in call_kwargs[0][0]
        assert result == [SAMPLE_MARKET_RECORD]

    def test_get_markets_passes_ids_param(self):
        client = CoinGeckoClient(base_url="https://fake-api.test")
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None

        with patch.object(client.session, "get", return_value=mock_response) as mock_get:
            client.get_markets(ids=["bitcoin", "ethereum"])

        params = mock_get.call_args[1]["params"]
        assert params["ids"] == "bitcoin,ethereum"

    def test_get_coin_history_calls_correct_url(self):
        client = CoinGeckoClient(base_url="https://fake-api.test")
        mock_response = MagicMock()
        mock_response.json.return_value = {"prices": [], "market_caps": [], "total_volumes": []}
        mock_response.raise_for_status.return_value = None

        with patch.object(client.session, "get", return_value=mock_response) as mock_get:
            result = client.get_coin_history("bitcoin", days=7)

        url = mock_get.call_args[0][0]
        assert "bitcoin/market_chart" in url
        assert "prices" in result


# ---------------------------------------------------------------------------
# BatchIngestionJob
# ---------------------------------------------------------------------------


class TestBatchIngestionJob:
    def _make_s3_client(self):
        s3 = MagicMock()
        s3.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        return s3

    _SENTINEL = object()

    def _make_api_client(self, records=_SENTINEL):
        api = MagicMock(spec=CoinGeckoClient)
        api.get_markets.return_value = (
            [SAMPLE_MARKET_RECORD] if records is self._SENTINEL else records
        )
        api.get_coin_history.return_value = {
            "prices": [[1_700_000_000_000, 50000]],
            "market_caps": [[1_700_000_000_000, 950_000_000_000]],
            "total_volumes": [[1_700_000_000_000, 30_000_000_000]],
        }
        return api

    def test_ingest_current_prices_writes_to_s3(self):
        s3 = self._make_s3_client()
        api = self._make_api_client()
        job = BatchIngestionJob(s3, "raw-bucket", SAMPLE_CONFIG, api_client=api)

        keys = job.ingest_current_prices()

        assert len(keys) == 1
        assert s3.put_object.call_count == 1
        key = keys[0]
        assert key.startswith("raw/prices/")
        assert "bitcoin" in key

    def test_ingest_current_prices_returns_empty_on_no_records(self):
        s3 = self._make_s3_client()
        api = self._make_api_client(records=[])
        job = BatchIngestionJob(s3, "raw-bucket", SAMPLE_CONFIG, api_client=api)

        keys = job.ingest_current_prices()

        assert keys == []
        s3.put_object.assert_not_called()

    def test_ingest_historical_prices_writes_payload(self):
        s3 = self._make_s3_client()
        api = self._make_api_client()
        job = BatchIngestionJob(s3, "raw-bucket", SAMPLE_CONFIG, api_client=api)

        key = job.ingest_historical_prices("bitcoin", days=7)

        assert "bitcoin" in key
        assert key.startswith("raw/history/")
        body = json.loads(s3.put_object.call_args[1]["Body"])
        assert body["symbol"] == "bitcoin"
        assert "data" in body

    def test_run_returns_summary_dict(self):
        s3 = self._make_s3_client()
        api = self._make_api_client()
        job = BatchIngestionJob(s3, "raw-bucket", SAMPLE_CONFIG, api_client=api)

        result = job.run()

        assert "price_keys" in result
        assert "history_keys" in result
        assert len(result["price_keys"]) == 1
        assert len(result["history_keys"]) == 2  # bitcoin + ethereum

    def test_run_continues_on_history_failure(self):
        """If history ingestion fails for one coin, the job should still continue."""
        s3 = self._make_s3_client()
        api = self._make_api_client()
        api.get_coin_history.side_effect = RuntimeError("API error")
        job = BatchIngestionJob(s3, "raw-bucket", SAMPLE_CONFIG, api_client=api)

        result = job.run()

        assert result["history_keys"] == []
