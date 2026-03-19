"""Unit tests for the real-time ingestion module."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.realtime_ingestion import KinesisProducer, RealtimeIngestionJob
from tests.test_batch_ingestion import SAMPLE_CONFIG, SAMPLE_MARKET_RECORD


# ---------------------------------------------------------------------------
# KinesisProducer
# ---------------------------------------------------------------------------


class TestKinesisProducer:
    def _make_client(self):
        client = MagicMock()
        client.put_record.return_value = {
            "ShardId": "shardId-000000000000",
            "SequenceNumber": "12345",
        }
        client.put_records.return_value = {
            "FailedRecordCount": 0,
            "Records": [{"ShardId": "shardId-000000000000", "SequenceNumber": "12345"}],
        }
        return client

    def test_put_record_uses_symbol_as_partition_key(self):
        client = self._make_client()
        producer = KinesisProducer(client, "test-stream")
        record = {"symbol": "bitcoin", "price_usd": 50000.0}

        producer.put_record(record)

        call_kwargs = client.put_record.call_args[1]
        assert call_kwargs["PartitionKey"] == "bitcoin"
        assert call_kwargs["StreamName"] == "test-stream"

    def test_put_record_serialises_data_as_json(self):
        client = self._make_client()
        producer = KinesisProducer(client, "test-stream")
        record = {"symbol": "ethereum", "price_usd": 3000.0}

        producer.put_record(record)

        data = client.put_record.call_args[1]["Data"]
        decoded = json.loads(data.decode("utf-8"))
        assert decoded["symbol"] == "ethereum"

    def test_put_records_batch_sends_all_records(self):
        client = self._make_client()
        producer = KinesisProducer(client, "test-stream")
        records = [
            {"symbol": "bitcoin", "price_usd": 50000.0},
            {"symbol": "ethereum", "price_usd": 3000.0},
        ]

        producer.put_records_batch(records)

        kinesis_records = client.put_records.call_args[1]["Records"]
        assert len(kinesis_records) == 2
        partition_keys = [r["PartitionKey"] for r in kinesis_records]
        assert "bitcoin" in partition_keys
        assert "ethereum" in partition_keys

    def test_put_records_batch_logs_warning_on_failures(self, caplog):
        client = self._make_client()
        client.put_records.return_value = {"FailedRecordCount": 1, "Records": []}
        producer = KinesisProducer(client, "test-stream")

        import logging
        with caplog.at_level(logging.WARNING, logger="src.ingestion.realtime_ingestion"):
            producer.put_records_batch([{"symbol": "bitcoin", "price_usd": 50000.0}])

        assert any("failed" in msg.lower() for msg in caplog.messages)


# ---------------------------------------------------------------------------
# RealtimeIngestionJob
# ---------------------------------------------------------------------------


class TestRealtimeIngestionJob:
    def _make_kinesis_client(self):
        client = MagicMock()
        client.put_records.return_value = {"FailedRecordCount": 0, "Records": []}
        return client

    def _make_api_client(self):
        from src.ingestion.batch_ingestion import CoinGeckoClient

        api = MagicMock(spec=CoinGeckoClient)
        api.get_markets.return_value = [SAMPLE_MARKET_RECORD]
        return api

    def test_fetch_and_publish_returns_normalised_records(self):
        kinesis = self._make_kinesis_client()
        api = self._make_api_client()
        job = RealtimeIngestionJob(
            kinesis_client=kinesis,
            stream_name="test-stream",
            config=SAMPLE_CONFIG,
            api_client=api,
        )

        records = job.fetch_and_publish()

        assert len(records) == 1
        assert records[0]["symbol"] == "bitcoin"
        assert records[0]["price_usd"] == 50000.0

    def test_fetch_and_publish_calls_kinesis(self):
        kinesis = self._make_kinesis_client()
        api = self._make_api_client()
        job = RealtimeIngestionJob(
            kinesis_client=kinesis,
            stream_name="test-stream",
            config=SAMPLE_CONFIG,
            api_client=api,
        )

        job.fetch_and_publish()

        kinesis.put_records.assert_called_once()

    def test_fetch_and_publish_no_records_skips_kinesis(self):
        kinesis = self._make_kinesis_client()
        api = self._make_api_client()
        api.get_markets.return_value = []
        job = RealtimeIngestionJob(
            kinesis_client=kinesis,
            stream_name="test-stream",
            config=SAMPLE_CONFIG,
            api_client=api,
        )

        records = job.fetch_and_publish()

        assert records == []
        kinesis.put_records.assert_not_called()

    def test_run_limited_iterations(self):
        kinesis = self._make_kinesis_client()
        api = self._make_api_client()
        config = {**SAMPLE_CONFIG, "kinesis": {"fetch_interval_seconds": 0}}
        job = RealtimeIngestionJob(
            kinesis_client=kinesis,
            stream_name="test-stream",
            config=config,
            api_client=api,
        )

        with patch("time.sleep"):
            job.run(max_iterations=3)

        assert kinesis.put_records.call_count == 3
