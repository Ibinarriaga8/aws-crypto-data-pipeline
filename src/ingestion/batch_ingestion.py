"""Batch ingestion module.

Fetches historical and current cryptocurrency market data from the
CoinGecko public API and stores raw JSON records in the S3 raw zone.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests
import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../config/config.yaml")


def load_config(config_path: str = _CONFIG_PATH) -> dict:
    """Load pipeline configuration from YAML file."""
    with open(config_path, "r") as fh:
        return yaml.safe_load(fh)


class CoinGeckoClient:
    """Thin wrapper around the CoinGecko REST API."""

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, base_url: str | None = None, timeout: int = 30) -> None:
        self.base_url = base_url or self.BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_markets(
        self,
        vs_currency: str = "usd",
        ids: list[str] | None = None,
        per_page: int = 100,
        page: int = 1,
    ) -> list[dict]:
        """Fetch current market data for a list of coins.

        Parameters
        ----------
        vs_currency:
            The target currency for pricing (default ``"usd"``).
        ids:
            Optional list of CoinGecko coin IDs to filter results.
        per_page:
            Number of results per page (max 250).
        page:
            Page number for pagination.

        Returns
        -------
        list[dict]
            Raw market data records returned by the API.
        """
        params: dict[str, Any] = {
            "vs_currency": vs_currency,
            "per_page": per_page,
            "page": page,
            "price_change_percentage": "24h",
            "sparkline": False,
        }
        if ids:
            params["ids"] = ",".join(ids)

        response = self.session.get(
            f"{self.base_url}/coins/markets",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_coin_history(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        days: int = 365,
    ) -> dict:
        """Fetch historical OHLCV data for a single coin.

        Parameters
        ----------
        coin_id:
            CoinGecko coin ID (e.g. ``"bitcoin"``).
        vs_currency:
            Target currency (default ``"usd"``).
        days:
            Number of days of history to retrieve.

        Returns
        -------
        dict
            Raw historical market chart data.
        """
        params = {
            "vs_currency": vs_currency,
            "days": days,
            "interval": "daily",
        }
        response = self.session.get(
            f"{self.base_url}/coins/{coin_id}/market_chart",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()


def normalise_market_record(raw: dict, ingestion_ts: str) -> dict:
    """Transform a raw CoinGecko market record into the pipeline schema.

    Parameters
    ----------
    raw:
        Single market data record as returned by :meth:`CoinGeckoClient.get_markets`.
    ingestion_ts:
        ISO-8601 UTC timestamp string to attach as ``ingestion_timestamp``.

    Returns
    -------
    dict
        Normalised record conforming to ``config/schema/crypto_schema.json``.
    """
    return {
        "symbol": raw.get("id", ""),
        "name": raw.get("name", ""),
        "price_usd": raw.get("current_price") or 0.0,
        "market_cap_usd": raw.get("market_cap") or 0.0,
        "volume_24h_usd": raw.get("total_volume") or 0.0,
        "price_change_24h": raw.get("price_change_24h") or 0.0,
        "price_change_pct_24h": raw.get("price_change_percentage_24h") or 0.0,
        "circulating_supply": raw.get("circulating_supply") or 0.0,
        "total_supply": raw.get("total_supply"),
        "ath": raw.get("ath") or 0.0,
        "ath_change_pct": raw.get("ath_change_percentage") or 0.0,
        "timestamp": raw.get("last_updated", ingestion_ts),
        "ingestion_timestamp": ingestion_ts,
        "source": "coingecko",
    }


class BatchIngestionJob:
    """Orchestrates batch ingestion of cryptocurrency market data into S3.

    Parameters
    ----------
    s3_client:
        A ``boto3`` S3 client instance.
    raw_bucket:
        Name of the S3 bucket where raw data should be stored.
    config:
        Pipeline configuration dictionary (loaded from ``config/config.yaml``).
    api_client:
        Optional :class:`CoinGeckoClient` instance (created automatically if omitted).
    """

    def __init__(
        self,
        s3_client,
        raw_bucket: str,
        config: dict,
        api_client: CoinGeckoClient | None = None,
    ) -> None:
        self.s3_client = s3_client
        self.raw_bucket = raw_bucket
        self.config = config
        self.api_client = api_client or CoinGeckoClient(
            base_url=config.get("crypto", {}).get("api_base_url")
        )

    def _s3_key(self, coin_id: str, record_type: str, ts: datetime) -> str:
        """Build a partitioned S3 key using Hive-style partitioning."""
        return (
            f"raw/{record_type}/year={ts.year}/month={ts.month:02d}/"
            f"day={ts.day:02d}/{coin_id}_{ts.strftime('%Y%m%dT%H%M%SZ')}.json"
        )

    def ingest_current_prices(self) -> list[str]:
        """Fetch current prices for all configured coins and write to S3.

        Returns
        -------
        list[str]
            S3 object keys that were written.
        """
        crypto_cfg = self.config.get("crypto", {})
        coin_ids: list[str] = crypto_cfg.get("symbols", [])
        vs_currency: str = crypto_cfg.get("vs_currency", "usd")

        ingestion_ts = datetime.now(timezone.utc).isoformat()
        ts = datetime.now(timezone.utc)

        raw_records = self.api_client.get_markets(
            vs_currency=vs_currency,
            ids=coin_ids,
        )

        written_keys: list[str] = []
        for raw in raw_records:
            normalised = normalise_market_record(raw, ingestion_ts)
            key = self._s3_key(normalised["symbol"], "prices", ts)
            self.s3_client.put_object(
                Bucket=self.raw_bucket,
                Key=key,
                Body=json.dumps(normalised),
                ContentType="application/json",
            )
            written_keys.append(key)
            logger.info("Written %s", key)

        logger.info("Ingested %d current price records.", len(written_keys))
        return written_keys

    def ingest_historical_prices(self, coin_id: str, days: int | None = None) -> str:
        """Fetch historical price data for a single coin and write to S3.

        Parameters
        ----------
        coin_id:
            CoinGecko coin identifier.
        days:
            Days of history to retrieve.  Defaults to ``config.crypto.historical_days``.

        Returns
        -------
        str
            S3 object key that was written.
        """
        crypto_cfg = self.config.get("crypto", {})
        vs_currency: str = crypto_cfg.get("vs_currency", "usd")
        days = days or crypto_cfg.get("historical_days", 365)

        ingestion_ts = datetime.now(timezone.utc).isoformat()
        ts = datetime.now(timezone.utc)

        historical = self.api_client.get_coin_history(
            coin_id=coin_id,
            vs_currency=vs_currency,
            days=days,
        )

        payload = {
            "symbol": coin_id,
            "vs_currency": vs_currency,
            "days": days,
            "ingestion_timestamp": ingestion_ts,
            "source": "coingecko",
            "data": historical,
        }

        key = self._s3_key(coin_id, "history", ts)
        self.s3_client.put_object(
            Bucket=self.raw_bucket,
            Key=key,
            Body=json.dumps(payload),
            ContentType="application/json",
        )
        logger.info("Written historical data for %s → %s", coin_id, key)
        return key

    def run(self) -> dict:
        """Execute a full batch ingestion cycle.

        Returns
        -------
        dict
            Summary with ``price_keys`` (list) and ``history_keys`` (list).
        """
        logger.info("Starting batch ingestion run.")
        price_keys = self.ingest_current_prices()

        crypto_cfg = self.config.get("crypto", {})
        coin_ids: list[str] = crypto_cfg.get("symbols", [])
        history_keys: list[str] = []
        for coin_id in coin_ids:
            try:
                key = self.ingest_historical_prices(coin_id)
                history_keys.append(key)
            except Exception:
                logger.exception("Failed to ingest history for %s", coin_id)

        logger.info(
            "Batch ingestion complete. Prices: %d, History: %d",
            len(price_keys),
            len(history_keys),
        )
        return {"price_keys": price_keys, "history_keys": history_keys}
