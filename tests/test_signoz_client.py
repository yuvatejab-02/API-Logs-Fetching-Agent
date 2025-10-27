"""Tests for SigNoz API client."""
import pytest
from src.signoz.api_client import SigNozClient
from src.utils.logger import setup_logging

setup_logging()


@pytest.fixture
def signoz_client():
    """Create SigNozClient instance."""
    return SigNozClient()


def test_connection(signoz_client):
    """Test SigNoz API connection."""
    result = signoz_client.test_connection()
    assert result is True, "SigNoz connection should succeed"


def test_fetch_logs(signoz_client):
    """Test fetching logs with a simple query."""
    from datetime import datetime, timedelta
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)
    
    query = {
        "start": int(start_time.timestamp() * 1000),
        "end": int(end_time.timestamp() * 1000),
        "requestType": "raw",
        "compositeQuery": {
            "queries": [{
                "type": "builder_query",
                "spec": {
                    "name": "A",
                    "signal": "logs",
                    "filter": {"expression": ""},
                    "limit": 10
                }
            }]
        }
    }
    
    response = signoz_client.fetch_logs(query)
    
    assert response is not None
    assert "data" in response
    print(f"\nFetched logs successfully")
