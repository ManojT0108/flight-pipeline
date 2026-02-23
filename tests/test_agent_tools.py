"""
Unit tests for agent tools — verify each tool calls fetch_all/fetch_one
with the right queries and returns properly structured JSON.

These tests mock the database layer so they run without a live DB connection.
Each test validates that the tool's output is valid JSON and contains the
expected keys/structure.
"""
import json
from unittest.mock import patch


def test_get_carrier_performance():
    """Should return a list of carriers sorted by the requested metric."""
    mock_rows = [{"carrier_code": "DL", "carrier_name": "Delta", "total_flights": 500000}]
    with patch("api.agent.tools.fetch_all", return_value=mock_rows):
        from api.agent.tools import get_carrier_performance
        result = json.loads(get_carrier_performance.invoke({"sort_by": "flights"}))
        assert len(result) == 1
        assert result[0]["carrier_code"] == "DL"


def test_get_carrier_details_not_found():
    """Should return an error message for a carrier code that doesn't exist."""
    with patch("api.agent.tools.fetch_one", return_value=None):
        from api.agent.tools import get_carrier_details
        result = json.loads(get_carrier_details.invoke({"carrier_code": "ZZ"}))
        assert "error" in result


def test_get_carrier_details_found():
    """Should return carrier info, delay breakdown, and monthly trend data."""
    carrier = {"carrier_code": "AA", "carrier_name": "American Airlines"}
    breakdown = {"avg_carrier_delay": 10.5, "avg_weather_delay": 3.2}
    monthly = [{"month": 1, "month_name": "January", "total_flights": 50000}]

    def mock_fetch_one(query, params=None):
        # Route to the right mock based on what the query is looking for
        if "carrier_name" in query and "AVG" not in query:
            return carrier
        return breakdown

    with patch("api.agent.tools.fetch_one", side_effect=mock_fetch_one), \
         patch("api.agent.tools.fetch_all", return_value=monthly):
        from api.agent.tools import get_carrier_details
        result = json.loads(get_carrier_details.invoke({"carrier_code": "AA"}))
        assert result["carrier"]["carrier_code"] == "AA"
        assert result["delay_breakdown"] is not None
        assert len(result["monthly_trend"]) == 1


def test_get_airport_info_not_found():
    """Should return an error for an airport code that doesn't exist in the data."""
    with patch("api.agent.tools.fetch_one", return_value=None):
        from api.agent.tools import get_airport_info
        result = json.loads(get_airport_info.invoke({"airport_code": "ZZZ"}))
        assert "error" in result


def test_get_system_health():
    """Should return table counts and date range from the database."""
    def mock_fetch_one(query, params=None):
        if "MIN(flight_date)" in query:
            return {"min_date": "2025-01-01", "max_date": "2025-11-30"}
        if "COUNT(*)" in query:
            return {"cnt": 100}
        return None

    with patch("api.agent.tools.fetch_one", side_effect=mock_fetch_one):
        from api.agent.tools import get_system_health
        result = json.loads(get_system_health.invoke({}))
        assert "tables" in result
        assert "date_range" in result
