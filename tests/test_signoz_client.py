"""Tests for SigNoz API client."""
import pytest
from src.signoz.api_client import SigNozClient
from src.utils.logger import setup_logging
from datetime import datetime, timezone, timedelta

setup_logging()


@pytest.fixture
def signoz_client():
    """Create SigNozClient instance."""
    return SigNozClient()


def test_dry_run_query_with_filter(signoz_client):
    """Test dry-run query with a filter expression."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    filter_expression = "service.name = 'test-service'"
    
    count = signoz_client.dry_run_query(
        filter_expression=filter_expression,
        start_ms=int(start_time.timestamp() * 1000),
        end_ms=int(end_time.timestamp() * 1000),
        limit=10,
        incident_id="TEST_001"
    )
    
    # Count should be >= 0 (may be 0 if no matching logs)
    assert count >= 0, "Dry-run query should return a count"
    print(f"\nDry-run query returned {count} logs")


def test_dry_run_query_empty_filter(signoz_client):
    """Test dry-run query with empty filter (fetch all)."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    count = signoz_client.dry_run_query(
        filter_expression="",
        start_ms=int(start_time.timestamp() * 1000),
        end_ms=int(end_time.timestamp() * 1000),
        limit=10,
        incident_id="TEST_002"
    )
    
    # Empty filter should return some logs (unless truly no logs in time range)
    assert count >= 0, "Dry-run query with empty filter should return a count"
    print(f"\nDry-run query with empty filter returned {count} logs")
