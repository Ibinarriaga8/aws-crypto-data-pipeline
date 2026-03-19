"""Real-time ingestion module.

Continuously polls the CoinGecko API and publishes normalised price
records to an Amazon Kinesis Data Stream for downstream real-time
processing.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

from src.ingestion.batch_ingestion import CoinGeckoClient, normalise_market_record

logger = logging.getLogger(__name__)


class KinesisProducer:
    """Puts cryptocurrency price records onto a Kinesis Data Stream.

    Parameters
    ----------
    kinesis_client:
        A ``boto3`` Kinesis client instance.
    stream_name:
        Name of the target Kinesis Data Stream.
    """

    def __init__(self, kinesis_client, stream_name: str) -> None:
        self.kinesis_client = kinesis_client
        self.stream_name = stream_name

    def put_record(self, record: dict) -> dict:
        """Publish a single record to the Kinesis stream.

        The ``symbol`` field is used as the partition key so that records
        for the same coin land on the same shard (preserving order).

        Parameters
        ----------
        record:
            Normalised price record conforming to the pipeline schema.

        Returns
        -------
        dict
            Raw boto3 response from ``put_record``.
        """
        partition_key = record.get("symbol", "unknown")
        response = self.kinesis_client.put_record(
            StreamName=self.stream_name,
            Data=json.dumps(record).encode("utf-8"),
            PartitionKey=partition_key,
        )
        logger.debug(
            "Put record for %s → shard %s / seq %s",
            partition_key,
            response.get("ShardId"),
            response.get("SequenceNumber"),
        )
        return response

    def put_records_batch(self, records: list[dict]) -> dict:
        """Publish a batch of records using ``put_records`` (up to 500 per call).

        Parameters
        ----------
        records:
            List of normalised price records.

        Returns
        -------
        dict
            Raw boto3 response from ``put_records``.
        """
        kinesis_records = [
            {
                "Data": json.dumps(r).encode("utf-8"),
                "PartitionKey": r.get("symbol", "unknown"),
            }
            for r in records
        ]
        response = self.kinesis_client.put_records(
            StreamName=self.stream_name,
            Records=kinesis_records,
        )
        failed = response.get("FailedRecordCount", 0)
        if failed:
            logger.warning("%d records failed to be put onto stream.", failed)
        return response


class RealtimeIngestionJob:
    """Polls the CoinGecko API at a fixed interval and streams data to Kinesis.

    Parameters
    ----------
    kinesis_client:
        A ``boto3`` Kinesis client instance.
    stream_name:
        Target Kinesis stream name.
    config:
        Pipeline configuration dictionary.
    api_client:
        Optional :class:`~src.ingestion.batch_ingestion.CoinGeckoClient`.
    """

    def __init__(
        self,
        kinesis_client,
        stream_name: str,
        config: dict,
        api_client: CoinGeckoClient | None = None,
    ) -> None:
        self.producer = KinesisProducer(kinesis_client, stream_name)
        self.config = config
        self.api_client = api_client or CoinGeckoClient(
            base_url=config.get("crypto", {}).get("api_base_url")
        )

    def fetch_and_publish(self) -> list[dict]:
        """Fetch current prices and publish each record to the Kinesis stream.

        Returns
        -------
        list[dict]
            The normalised records that were published.
        """
        crypto_cfg = self.config.get("crypto", {})
        coin_ids: list[str] = crypto_cfg.get("symbols", [])
        vs_currency: str = crypto_cfg.get("vs_currency", "usd")

        ingestion_ts = datetime.now(timezone.utc).isoformat()
        raw_records = self.api_client.get_markets(
            vs_currency=vs_currency,
            ids=coin_ids,
        )

        normalised = [normalise_market_record(r, ingestion_ts) for r in raw_records]
        if normalised:
            self.producer.put_records_batch(normalised)
        logger.info("Published %d records to Kinesis.", len(normalised))
        return normalised

    def run(self, max_iterations: int | None = None) -> None:
        """Start the polling loop.

        Parameters
        ----------
        max_iterations:
            If set, stop after this many iterations (useful for testing).
            If ``None``, loop indefinitely.
        """
        interval = self.config.get("kinesis", {}).get(
            "fetch_interval_seconds",
            self.config.get("crypto", {}).get("fetch_interval_seconds", 60),
        )
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            try:
                self.fetch_and_publish()
            except Exception:
                logger.exception("Error during real-time ingestion cycle.")
            iteration += 1
            if max_iterations is None or iteration < max_iterations:
                time.sleep(interval)
