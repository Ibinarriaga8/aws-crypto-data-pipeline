"""S3 storage handler.

Provides helper methods for reading and writing data in the three-zone
data lake layout (raw → processed → curated) stored in Amazon S3.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Supported serialisation formats
FORMATS = {"json", "parquet", "csv"}


class S3Handler:
    """High-level wrapper for S3 data lake operations.

    Parameters
    ----------
    s3_client:
        A ``boto3`` S3 client instance.
    raw_bucket:
        Name of the raw-zone bucket.
    processed_bucket:
        Name of the processed-zone bucket.
    curated_bucket:
        Name of the curated-zone bucket.
    """

    def __init__(
        self,
        s3_client,
        raw_bucket: str,
        processed_bucket: str,
        curated_bucket: str,
    ) -> None:
        self.s3_client = s3_client
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.curated_bucket = curated_bucket

    # ------------------------------------------------------------------
    # Generic read / write helpers
    # ------------------------------------------------------------------

    def put_json(self, bucket: str, key: str, data: Any) -> None:
        """Serialise *data* as JSON and upload to S3.

        Parameters
        ----------
        bucket:
            Target S3 bucket name.
        key:
            S3 object key.
        data:
            JSON-serialisable Python object.
        """
        body = json.dumps(data, default=str).encode("utf-8")
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        logger.debug("Uploaded JSON → s3://%s/%s", bucket, key)

    def get_json(self, bucket: str, key: str) -> Any:
        """Download and deserialise a JSON object from S3.

        Parameters
        ----------
        bucket:
            Source S3 bucket name.
        key:
            S3 object key.

        Returns
        -------
        Any
            Deserialised Python object.
        """
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))

    def put_parquet(self, bucket: str, key: str, dataframe) -> None:
        """Serialise a ``pandas.DataFrame`` as Parquet and upload to S3.

        Parameters
        ----------
        bucket:
            Target S3 bucket name.
        key:
            S3 object key (should end with ``.parquet``).
        dataframe:
            ``pandas.DataFrame`` to serialise.
        """
        import pandas as pd  # noqa: F401 – imported for type safety

        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.debug("Uploaded Parquet → s3://%s/%s", bucket, key)

    def get_parquet(self, bucket: str, key: str):
        """Download a Parquet object from S3 and return as a ``pandas.DataFrame``.

        Parameters
        ----------
        bucket:
            Source S3 bucket name.
        key:
            S3 object key.

        Returns
        -------
        pandas.DataFrame
        """
        import pandas as pd

        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer, engine="pyarrow")

    # ------------------------------------------------------------------
    # Listing helpers
    # ------------------------------------------------------------------

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        """List all object keys under *prefix* in *bucket*.

        Parameters
        ----------
        bucket:
            S3 bucket name.
        prefix:
            Key prefix to filter objects.

        Returns
        -------
        list[str]
            Sorted list of matching object keys.
        """
        keys: list[str] = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return sorted(keys)

    # ------------------------------------------------------------------
    # Zone-specific convenience methods
    # ------------------------------------------------------------------

    def _partition_key(self, zone: str, record_type: str, ts: datetime, filename: str) -> str:
        return (
            f"{zone}/{record_type}/year={ts.year}/month={ts.month:02d}/"
            f"day={ts.day:02d}/{filename}"
        )

    def write_raw(self, record_type: str, filename: str, data: Any) -> str:
        """Write a raw JSON record to the raw-zone bucket.

        Parameters
        ----------
        record_type:
            Logical dataset name (e.g. ``"prices"``).
        filename:
            Base filename (without path).
        data:
            JSON-serialisable payload.

        Returns
        -------
        str
            The S3 key that was written.
        """
        ts = datetime.now(timezone.utc)
        key = self._partition_key("raw", record_type, ts, filename)
        self.put_json(self.raw_bucket, key, data)
        return key

    def write_processed(self, record_type: str, filename: str, dataframe) -> str:
        """Write a processed Parquet file to the processed-zone bucket.

        Parameters
        ----------
        record_type:
            Logical dataset name (e.g. ``"prices"``).
        filename:
            Base filename (should end with ``.parquet``).
        dataframe:
            ``pandas.DataFrame`` to serialise.

        Returns
        -------
        str
            The S3 key that was written.
        """
        ts = datetime.now(timezone.utc)
        key = self._partition_key("processed", record_type, ts, filename)
        self.put_parquet(self.processed_bucket, key, dataframe)
        return key

    def write_curated(self, record_type: str, filename: str, dataframe) -> str:
        """Write a curated Parquet file to the curated-zone bucket.

        Parameters
        ----------
        record_type:
            Logical dataset name (e.g. ``"prices_aggregated"``).
        filename:
            Base filename.
        dataframe:
            ``pandas.DataFrame`` containing curated analytics-ready data.

        Returns
        -------
        str
            The S3 key that was written.
        """
        ts = datetime.now(timezone.utc)
        key = self._partition_key("curated", record_type, ts, filename)
        self.put_parquet(self.curated_bucket, key, dataframe)
        return key

    def move_to_processed(self, raw_key: str, processed_key: str) -> None:
        """Copy an object from the raw bucket to the processed bucket.

        This is a server-side copy; the original object is not deleted.

        Parameters
        ----------
        raw_key:
            Source key in the raw bucket.
        processed_key:
            Destination key in the processed bucket.
        """
        copy_source = {"Bucket": self.raw_bucket, "Key": raw_key}
        self.s3_client.copy_object(
            CopySource=copy_source,
            Bucket=self.processed_bucket,
            Key=processed_key,
        )
        logger.debug(
            "Copied s3://%s/%s → s3://%s/%s",
            self.raw_bucket,
            raw_key,
            self.processed_bucket,
            processed_key,
        )
