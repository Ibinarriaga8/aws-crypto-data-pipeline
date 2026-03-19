"""Batch ETL processor.

Reads raw JSON price records from S3, applies cleansing and enrichment
transformations, and writes the result as Parquet to the processed zone.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.storage.s3_handler import S3Handler

logger = logging.getLogger(__name__)


def parse_raw_records(raw_objects: list[Any]) -> pd.DataFrame:
    """Parse a list of raw JSON-decoded objects into a ``DataFrame``.

    Parameters
    ----------
    raw_objects:
        List of dicts (one per price record) as returned by
        :meth:`S3Handler.get_json`.

    Returns
    -------
    pandas.DataFrame
        Flat DataFrame with one row per record.
    """
    records = []
    for obj in raw_objects:
        if isinstance(obj, dict):
            records.append(obj)
        elif isinstance(obj, list):
            records.extend(obj)
    return pd.DataFrame(records)


def cleanse(df: pd.DataFrame) -> pd.DataFrame:
    """Apply data quality checks and fixes.

    Steps
    -----
    1. Drop rows where ``symbol`` or ``price_usd`` is null / zero.
    2. Parse ``timestamp`` and ``ingestion_timestamp`` as UTC datetimes.
    3. Cast numeric columns to ``float64``.
    4. Deduplicate by ``(symbol, timestamp)``, keeping the latest ingestion.

    Parameters
    ----------
    df:
        Raw ``DataFrame``.

    Returns
    -------
    pandas.DataFrame
        Cleansed ``DataFrame``.
    """
    if df.empty:
        return df

    df = df.copy()

    # Drop rows without a usable price
    df = df[df["symbol"].notna() & (df["price_usd"].fillna(0) > 0)]

    # Parse timestamps
    for col in ("timestamp", "ingestion_timestamp"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Cast numeric columns
    numeric_cols = [
        "price_usd",
        "market_cap_usd",
        "volume_24h_usd",
        "price_change_24h",
        "price_change_pct_24h",
        "circulating_supply",
        "total_supply",
        "ath",
        "ath_change_pct",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Deduplicate – keep the row with the latest ingestion_timestamp
    if "ingestion_timestamp" in df.columns and "timestamp" in df.columns:
        df = (
            df.sort_values("ingestion_timestamp", ascending=False)
            .drop_duplicates(subset=["symbol", "timestamp"])
            .reset_index(drop=True)
        )

    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns useful for analytics.

    Derived columns
    ---------------
    * ``market_cap_rank`` – rank by market cap descending (within the batch).
    * ``price_usd_log`` – natural log of the price (for scale-invariant analysis).
    * ``processing_timestamp`` – UTC timestamp when ETL was applied.

    Parameters
    ----------
    df:
        Cleansed ``DataFrame``.

    Returns
    -------
    pandas.DataFrame
        Enriched ``DataFrame``.
    """
    if df.empty:
        return df

    import numpy as np

    df = df.copy()
    df["market_cap_rank"] = (
        df["market_cap_usd"].rank(ascending=False, method="min").astype("Int64")
        if "market_cap_usd" in df.columns
        else pd.NA
    )
    if "price_usd" in df.columns:
        df["price_usd_log"] = np.log(df["price_usd"].clip(lower=1e-12))
    df["processing_timestamp"] = datetime.now(timezone.utc).isoformat()

    return df


class BatchProcessor:
    """Read raw records from S3, transform them, and write processed Parquet.

    Parameters
    ----------
    s3_handler:
        An :class:`~src.storage.s3_handler.S3Handler` instance.
    config:
        Pipeline configuration dictionary.
    """

    def __init__(self, s3_handler: S3Handler, config: dict) -> None:
        self.s3_handler = s3_handler
        self.config = config

    def process_raw_prefix(self, prefix: str = "raw/prices/") -> str | None:
        """Process all raw JSON objects under *prefix* and write Parquet output.

        Parameters
        ----------
        prefix:
            S3 key prefix in the raw bucket to scan.

        Returns
        -------
        str or None
            S3 key of the written Parquet file, or ``None`` if there was
            nothing to process.
        """
        raw_keys = self.s3_handler.list_objects(
            self.s3_handler.raw_bucket, prefix=prefix
        )
        if not raw_keys:
            logger.info("No raw objects found under prefix '%s'.", prefix)
            return None

        logger.info("Processing %d raw objects from prefix '%s'.", len(raw_keys), prefix)
        all_records: list[Any] = []
        for key in raw_keys:
            try:
                data = self.s3_handler.get_json(self.s3_handler.raw_bucket, key)
                all_records.append(data)
            except Exception:
                logger.exception("Failed to read %s", key)

        df = parse_raw_records(all_records)
        df = cleanse(df)
        df = enrich(df)

        if df.empty:
            logger.warning("No valid records after cleansing; skipping write.")
            return None

        ts = datetime.now(timezone.utc)
        filename = f"prices_{ts.strftime('%Y%m%dT%H%M%SZ')}.parquet"
        key = self.s3_handler.write_processed("prices", filename, df)
        logger.info("Wrote %d rows to %s", len(df), key)
        return key

    def run(self) -> dict:
        """Execute a full batch ETL cycle for current price data.

        Returns
        -------
        dict
            ``{"processed_key": str | None, "row_count": int}``
        """
        logger.info("Starting batch ETL job.")
        processed_key = self.process_raw_prefix("raw/prices/")
        result = {
            "processed_key": processed_key,
            "row_count": 0,
        }
        if processed_key:
            df = self.s3_handler.get_parquet(
                self.s3_handler.processed_bucket, processed_key
            )
            result["row_count"] = len(df)
        logger.info("Batch ETL complete: %s", result)
        return result
