"""Unit tests for the Athena analytics module."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from src.analytics.athena_queries import AthenaQueryRunner, CryptoAnalytics


# ---------------------------------------------------------------------------
# AthenaQueryRunner
# ---------------------------------------------------------------------------


def _make_client(state="SUCCEEDED", rows=None):
    """Create a mock Athena client that returns the given state and rows."""
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "qid-123"}
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": state}}
    }

    # Build mock paginator results
    if rows is None:
        rows = [
            {"Data": [{"VarCharValue": "symbol"}, {"VarCharValue": "price_usd"}]},
            {"Data": [{"VarCharValue": "bitcoin"}, {"VarCharValue": "50000"}]},
        ]
    mock_page = {"ResultSet": {"Rows": rows}}
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [mock_page]
    client.get_paginator.return_value = mock_paginator

    return client


def _make_runner(client=None, **kwargs):
    client = client or _make_client()
    return AthenaQueryRunner(
        athena_client=client,
        database="crypto_pipeline_db",
        output_location="s3://results/",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# start_query
# ---------------------------------------------------------------------------


def test_start_query_returns_execution_id():
    runner = _make_runner()
    exec_id = runner.start_query("SELECT 1")
    assert exec_id == "qid-123"


def test_start_query_passes_database_and_output():
    client = _make_client()
    runner = _make_runner(client)
    runner.start_query("SELECT 1")
    call_kwargs = client.start_query_execution.call_args[1]
    assert call_kwargs["QueryExecutionContext"]["Database"] == "crypto_pipeline_db"
    assert call_kwargs["ResultConfiguration"]["OutputLocation"] == "s3://results/"


# ---------------------------------------------------------------------------
# wait_for_query
# ---------------------------------------------------------------------------


def test_wait_for_query_returns_succeeded():
    runner = _make_runner()
    state = runner.wait_for_query("qid-123")
    assert state == "SUCCEEDED"


def test_wait_for_query_raises_on_failed():
    client = _make_client(state="FAILED")
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "FAILED",
                "StateChangeReason": "Syntax error",
            }
        }
    }
    runner = _make_runner(client)
    with pytest.raises(RuntimeError, match="FAILED"):
        runner.wait_for_query("qid-123")


def test_wait_for_query_raises_on_cancelled():
    client = _make_client(state="CANCELLED")
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {
                "State": "CANCELLED",
                "StateChangeReason": "User cancelled",
            }
        }
    }
    runner = _make_runner(client)
    with pytest.raises(RuntimeError, match="CANCELLED"):
        runner.wait_for_query("qid-123")


def test_wait_for_query_timeout():
    from src.analytics import athena_queries as aq

    client = _make_client(state="RUNNING")
    runner = _make_runner(client)

    original_max = aq._MAX_WAIT_SECONDS
    original_poll = aq._POLL_INTERVAL_SECONDS
    aq._MAX_WAIT_SECONDS = 0
    aq._POLL_INTERVAL_SECONDS = 1
    try:
        with pytest.raises(TimeoutError):
            runner.wait_for_query("qid-123")
    finally:
        aq._MAX_WAIT_SECONDS = original_max
        aq._POLL_INTERVAL_SECONDS = original_poll


# ---------------------------------------------------------------------------
# get_results
# ---------------------------------------------------------------------------


def test_get_results_returns_list_of_dicts():
    runner = _make_runner()
    results = runner.get_results("qid-123")
    assert results == [{"symbol": "bitcoin", "price_usd": "50000"}]


def test_get_results_empty_rows():
    client = _make_client(
        rows=[
            {"Data": [{"VarCharValue": "symbol"}]},
            # No data rows
        ]
    )
    runner = _make_runner(client)
    results = runner.get_results("qid-123")
    assert results == []


# ---------------------------------------------------------------------------
# run (end-to-end shortcut)
# ---------------------------------------------------------------------------


def test_run_returns_results():
    runner = _make_runner()
    results = runner.run("SELECT * FROM prices")
    assert len(results) == 1
    assert results[0]["symbol"] == "bitcoin"


# ---------------------------------------------------------------------------
# CryptoAnalytics
# ---------------------------------------------------------------------------


class TestCryptoAnalytics:
    def _make_analytics(self, results=None):
        runner = MagicMock(spec=AthenaQueryRunner)
        runner.run.return_value = results or [{"symbol": "bitcoin", "price_usd": "50000"}]
        return CryptoAnalytics(runner)

    def test_top_by_market_cap_calls_runner(self):
        analytics = self._make_analytics()
        result = analytics.top_by_market_cap(limit=5)
        analytics.runner.run.assert_called_once()
        sql = analytics.runner.run.call_args[0][0]
        assert "market_cap_usd" in sql
        assert "LIMIT 5" in sql

    def test_price_changes_24h_calls_runner(self):
        analytics = self._make_analytics()
        analytics.price_changes_24h()
        analytics.runner.run.assert_called_once()
        sql = analytics.runner.run.call_args[0][0]
        assert "price_change_pct_24h" in sql

    def test_volume_leaders_calls_runner(self):
        analytics = self._make_analytics()
        analytics.volume_leaders(limit=3)
        sql = analytics.runner.run.call_args[0][0]
        assert "volume_24h_usd" in sql
        assert "LIMIT 3" in sql

    def test_daily_price_summary_includes_symbol(self):
        analytics = self._make_analytics()
        analytics.daily_price_summary("bitcoin", days=7)
        sql = analytics.runner.run.call_args[0][0]
        assert "bitcoin" in sql
        assert "7" in sql

    def test_market_dominance_includes_total(self):
        analytics = self._make_analytics()
        analytics.market_dominance()
        sql = analytics.runner.run.call_args[0][0]
        assert "SUM(market_cap_usd)" in sql
        assert "dominance_pct" in sql
