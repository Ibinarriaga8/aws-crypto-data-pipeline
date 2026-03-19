"""Athena analytics module.

Provides helper classes for running SQL queries against the crypto pipeline
data lake via Amazon Athena and retrieving aggregated insights.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 2
_MAX_WAIT_SECONDS = 300


class AthenaQueryRunner:
    """Run SQL queries on Athena and wait for results.

    Parameters
    ----------
    athena_client:
        A ``boto3`` Athena client.
    database:
        Athena database (Glue Data Catalog) name.
    output_location:
        S3 URI where query results are stored
        (e.g. ``"s3://my-bucket/athena-results/"``).
    workgroup:
        Athena workgroup name (default ``"primary"``).
    """

    def __init__(
        self,
        athena_client,
        database: str,
        output_location: str,
        workgroup: str = "primary",
    ) -> None:
        self.athena_client = athena_client
        self.database = database
        self.output_location = output_location
        self.workgroup = workgroup

    def start_query(self, sql: str) -> str:
        """Submit a query and return its execution ID.

        Parameters
        ----------
        sql:
            SQL statement to execute.

        Returns
        -------
        str
            Athena query execution ID.
        """
        response = self.athena_client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.workgroup,
        )
        execution_id = response["QueryExecutionId"]
        logger.debug("Started Athena query: %s", execution_id)
        return execution_id

    def wait_for_query(self, execution_id: str) -> str:
        """Poll until the query finishes and return its final state.

        Parameters
        ----------
        execution_id:
            Athena query execution ID.

        Returns
        -------
        str
            Final state: ``"SUCCEEDED"``, ``"FAILED"``, or ``"CANCELLED"``.

        Raises
        ------
        TimeoutError
            If the query does not complete within ``_MAX_WAIT_SECONDS``.
        RuntimeError
            If the query fails or is cancelled.
        """
        elapsed = 0.0
        while elapsed < _MAX_WAIT_SECONDS:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )
            state = response["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                if state != "SUCCEEDED":
                    reason = (
                        response["QueryExecution"]["Status"]
                        .get("StateChangeReason", "Unknown error")
                    )
                    raise RuntimeError(
                        f"Athena query {execution_id} {state}: {reason}"
                    )
                return state
            time.sleep(_POLL_INTERVAL_SECONDS)
            elapsed += _POLL_INTERVAL_SECONDS

        raise TimeoutError(
            f"Athena query {execution_id} did not complete within {_MAX_WAIT_SECONDS}s."
        )

    def get_results(self, execution_id: str) -> list[dict]:
        """Retrieve query results as a list of row dicts.

        Parameters
        ----------
        execution_id:
            Athena query execution ID (must be in ``SUCCEEDED`` state).

        Returns
        -------
        list[dict]
            Each element is a ``{column_name: value}`` dict for one row.
        """
        results = []
        paginator = self.athena_client.get_paginator("get_query_results")
        first_page = True
        headers: list[str] = []

        for page in paginator.paginate(QueryExecutionId=execution_id):
            rows = page["ResultSet"]["Rows"]
            if first_page and rows:
                headers = [col["VarCharValue"] for col in rows[0]["Data"]]
                rows = rows[1:]
                first_page = False
            for row in rows:
                values = [cell.get("VarCharValue", "") for cell in row["Data"]]
                results.append(dict(zip(headers, values)))

        return results

    def run(self, sql: str) -> list[dict]:
        """Submit *sql*, wait for completion, and return rows.

        Parameters
        ----------
        sql:
            SQL statement to execute.

        Returns
        -------
        list[dict]
            Query result rows.
        """
        execution_id = self.start_query(sql)
        self.wait_for_query(execution_id)
        return self.get_results(execution_id)


# ---------------------------------------------------------------------------
# Pre-built analytical queries
# ---------------------------------------------------------------------------

class CryptoAnalytics:
    """Collection of common analytical queries for the crypto pipeline.

    Parameters
    ----------
    runner:
        An :class:`AthenaQueryRunner` instance.
    processed_table:
        Name of the Athena table containing processed price data.
    """

    def __init__(self, runner: AthenaQueryRunner, processed_table: str = "crypto_prices_processed") -> None:
        self.runner = runner
        self.processed_table = processed_table

    def top_by_market_cap(self, limit: int = 10) -> list[dict]:
        """Return the top *limit* coins ordered by market capitalisation.

        Parameters
        ----------
        limit:
            Number of rows to return.

        Returns
        -------
        list[dict]
        """
        sql = f"""
            SELECT symbol, name, price_usd, market_cap_usd, volume_24h_usd
            FROM {self.processed_table}
            WHERE market_cap_usd IS NOT NULL
            ORDER BY market_cap_usd DESC
            LIMIT {int(limit)}
        """
        return self.runner.run(sql)

    def price_changes_24h(self) -> list[dict]:
        """Return 24-hour price change percentages sorted descending.

        Returns
        -------
        list[dict]
        """
        sql = f"""
            SELECT symbol, name, price_usd, price_change_pct_24h
            FROM {self.processed_table}
            ORDER BY price_change_pct_24h DESC
        """
        return self.runner.run(sql)

    def volume_leaders(self, limit: int = 10) -> list[dict]:
        """Return the top *limit* coins by 24-hour trading volume.

        Parameters
        ----------
        limit:
            Number of rows to return.

        Returns
        -------
        list[dict]
        """
        sql = f"""
            SELECT symbol, name, volume_24h_usd, price_usd
            FROM {self.processed_table}
            WHERE volume_24h_usd IS NOT NULL
            ORDER BY volume_24h_usd DESC
            LIMIT {int(limit)}
        """
        return self.runner.run(sql)

    def daily_price_summary(self, symbol: str, days: int = 30) -> list[dict]:
        """Return a daily OHLCV-style summary for a single coin.

        Parameters
        ----------
        symbol:
            CoinGecko coin ID (e.g. ``"bitcoin"``).
        days:
            Look-back window in days.

        Returns
        -------
        list[dict]
        """
        sql = f"""
            SELECT
                DATE(timestamp) AS date,
                symbol,
                MIN(price_usd) AS price_low,
                MAX(price_usd) AS price_high,
                AVG(price_usd) AS price_avg,
                SUM(volume_24h_usd) AS total_volume
            FROM {self.processed_table}
            WHERE symbol = '{symbol}'
              AND timestamp >= DATE_ADD('day', -{int(days)}, CURRENT_DATE)
            GROUP BY DATE(timestamp), symbol
            ORDER BY date DESC
        """
        return self.runner.run(sql)

    def market_dominance(self) -> list[dict]:
        """Compute each coin's share of the total market capitalisation.

        Returns
        -------
        list[dict]
        """
        sql = f"""
            WITH totals AS (
                SELECT SUM(market_cap_usd) AS total_market_cap
                FROM {self.processed_table}
                WHERE market_cap_usd IS NOT NULL
            )
            SELECT
                p.symbol,
                p.name,
                p.market_cap_usd,
                ROUND(100.0 * p.market_cap_usd / t.total_market_cap, 4) AS dominance_pct
            FROM {self.processed_table} p
            CROSS JOIN totals t
            WHERE p.market_cap_usd IS NOT NULL
            ORDER BY dominance_pct DESC
        """
        return self.runner.run(sql)
