"""Tests for SigNozFetcher multi-signal functionality."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.signoz.fetcher import SigNozFetcher


class TestSigNozFetcher:
    """Test SigNozFetcher for logs, metrics, and traces."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a SigNozFetcher instance."""
        return SigNozFetcher(
            api_endpoint="https://api.signoz.io",
            api_key="test-key",
            timeout=30
        )
    
    @pytest.fixture
    def mock_response(self):
        """Create a mock response."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.elapsed.total_seconds.return_value = 0.5
        mock_resp.json.return_value = {
            "data": {
                "data": {
                    "results": [{
                        "rows": [
                            {"data": {"id": "1", "body": "test log"}},
                            {"data": {"id": "2", "body": "another log"}}
                        ]
                    }]
                }
            }
        }
        return mock_resp
    
    @patch('src.signoz.fetcher.requests.post')
    def test_fetch_logs(self, mock_post, fetcher, mock_response):
        """Test fetching logs."""
        mock_post.return_value = mock_response
        
        result = fetcher.fetch_logs(
            start_ms=1000000,
            end_ms=2000000,
            filter_expression="service.name = 'test'",
            limit=100,
            incident_id="INC_001"
        )
        
        assert result is not None
        assert "data" in result
        mock_post.assert_called_once()
        
        # Verify payload structure
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        assert payload['start'] == 1000000
        assert payload['end'] == 2000000
        assert payload['compositeQuery']['queries'][0]['spec']['signal'] == 'logs'
        assert payload['compositeQuery']['queries'][0]['spec']['limit'] == 100
    
    @patch('src.signoz.fetcher.requests.post')
    def test_fetch_metrics(self, mock_post, fetcher):
        """Test fetching metrics."""
        mock_resp = Mock()
        mock_resp.status_code = 200
        mock_resp.elapsed.total_seconds.return_value = 0.3
        mock_resp.json.return_value = {
            "data": {
                "data": {
                    "results": [{
                        "series": [
                            {"labels": {"service": "api"}, "values": [[1000, 100]]}
                        ]
                    }]
                }
            }
        }
        mock_post.return_value = mock_resp
        
        result = fetcher.fetch_metrics(
            start_ms=1000000,
            end_ms=2000000,
            metric_name="http_request_duration",
            aggregation="p95",
            group_by=["service.name"],
            step_seconds=60,
            incident_id="INC_001"
        )
        
        assert result is not None
        mock_post.assert_called_once()
        
        # Verify payload structure (v5 API format)
        payload = mock_post.call_args[1]['json']
        assert payload['requestType'] == 'time_series'
        assert payload['compositeQuery']['queries'][0]['spec']['signal'] == 'metrics'
        assert payload['compositeQuery']['queries'][0]['spec']['aggregations'][0]['metricName'] == 'http_request_duration'
        assert payload['compositeQuery']['queries'][0]['spec']['aggregations'][0]['timeAggregation'] == 'p95'
        assert payload['compositeQuery']['queries'][0]['spec']['stepInterval'] == 60
    
    @patch('src.signoz.fetcher.requests.post')
    def test_fetch_traces(self, mock_post, fetcher, mock_response):
        """Test fetching traces."""
        mock_post.return_value = mock_response
        
        result = fetcher.fetch_traces(
            start_ms=1000000,
            end_ms=2000000,
            filter_expression="service.name = 'api'",
            limit=500,
            order_by="duration",
            incident_id="INC_001"
        )
        
        assert result is not None
        mock_post.assert_called_once()
        
        # Verify payload structure
        payload = mock_post.call_args[1]['json']
        assert payload['compositeQuery']['queries'][0]['spec']['signal'] == 'traces'
        assert payload['compositeQuery']['queries'][0]['spec']['limit'] == 500
        assert payload['compositeQuery']['queries'][0]['spec']['order'][0]['key']['name'] == 'duration'
    
    @patch('src.signoz.fetcher.requests.post')
    def test_fetch_all_signals(self, mock_post, fetcher, mock_response):
        """Test fetching all signals (logs and traces)."""
        mock_post.return_value = mock_response
        
        result = fetcher.fetch_all_signals(
            start_ms=1000000,
            end_ms=2000000,
            filter_expression="",
            logs_limit=1000,
            traces_limit=1000,
            incident_id="INC_001"
        )
        
        assert "logs" in result
        assert "traces" in result
        assert mock_post.call_count == 2  # logs + traces
    
    @patch('src.signoz.fetcher.requests.post')
    def test_fetch_logs_with_empty_filter(self, mock_post, fetcher, mock_response):
        """Test fetching logs with empty filter (ALL mode)."""
        mock_post.return_value = mock_response
        
        result = fetcher.fetch_logs(
            start_ms=1000000,
            end_ms=2000000,
            filter_expression="",
            limit=1000
        )
        
        assert result is not None
        payload = mock_post.call_args[1]['json']
        assert payload['compositeQuery']['queries'][0]['spec']['filter']['expression'] == ""
    
    @patch('src.signoz.fetcher.requests.post')
    def test_api_error_handling(self, mock_post, fetcher):
        """Test API error handling."""
        mock_resp = Mock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_resp.elapsed.total_seconds.return_value = 0.5
        mock_resp.raise_for_status.side_effect = Exception("API Error")
        mock_post.return_value = mock_resp
        
        with pytest.raises(Exception) as exc_info:
            fetcher.fetch_logs(
                start_ms=1000000,
                end_ms=2000000,
                filter_expression="test"
            )
        
        assert "API Error" in str(exc_info.value) or "Failed to fetch" in str(exc_info.value)
    
    @patch('src.signoz.fetcher.requests.post')
    def test_timeout_handling(self, mock_post, fetcher):
        """Test timeout handling."""
        import requests
        mock_post.side_effect = requests.exceptions.Timeout()
        
        with pytest.raises(Exception) as exc_info:
            fetcher.fetch_logs(
                start_ms=1000000,
                end_ms=2000000,
                filter_expression="test"
            )
        
        assert "timed out" in str(exc_info.value)
    
    def test_extract_count_logs(self, fetcher):
        """Test extracting count from logs response."""
        response_data = {
            "data": {
                "data": {
                    "results": [{
                        "rows": [{"id": "1"}, {"id": "2"}, {"id": "3"}]
                    }]
                }
            }
        }
        
        count = fetcher._extract_count(response_data, "logs")
        assert count == 3
    
    def test_extract_count_metrics(self, fetcher):
        """Test extracting count from metrics response."""
        response_data = {
            "data": {
                "data": {
                    "results": [{
                        "series": [
                            {"labels": {}, "values": []},
                            {"labels": {}, "values": []}
                        ]
                    }]
                }
            }
        }
        
        count = fetcher._extract_count(response_data, "metrics")
        assert count == 2
    
    def test_extract_count_empty_response(self, fetcher):
        """Test extracting count from empty response."""
        response_data = {"data": {"data": {"results": []}}}
        
        count = fetcher._extract_count(response_data, "logs")
        assert count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

