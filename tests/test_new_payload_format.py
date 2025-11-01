"""Test new payload format validation and extraction."""
import pytest
from src.utils.sqs_schema import validate_and_extract_payload


class TestNewPayloadFormat:
    """Test suite for new payload format."""
    
    def test_valid_payload_extraction(self):
        """Test extraction of valid payload."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {
                        "api_endpoint": "https://test-signoz.us.signoz.cloud"
                    },
                    "auth_config": {
                        "auth_type": "api_key",
                        "api_key": "test_key_12345"
                    }
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test incident",
                "service": {
                    "name": "test-service"
                },
                "environment": "prod",
                "lookback_hours": 1
            }
        }
        
        incident, endpoint, api_key = validate_and_extract_payload(payload)
        
        assert incident["incident_id"] == "INC_test_001"
        assert incident["company_id"] == "testcompany"
        assert incident["service"]["name"] == "test-service"
        assert endpoint == "https://test-signoz.us.signoz.cloud"
        assert api_key == "test_key_12345"
    
    def test_missing_job_type(self):
        """Test validation fails when job_type is missing."""
        payload = {
            "data_sources": [],
            "incident": {}
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_missing_data_sources(self):
        """Test validation fails when data_sources is missing."""
        payload = {
            "job_type": "incident-data-fetch",
            "incident": {}
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_empty_data_sources(self):
        """Test validation fails when data_sources is empty."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):  # Schema validation will fail
            validate_and_extract_payload(payload)
    
    def test_multiple_data_sources(self):
        """Test validation fails when multiple data sources provided."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {"api_endpoint": "https://test1.signoz.cloud"},
                    "auth_config": {"auth_type": "api_key", "api_key": "key1"}
                },
                {
                    "source_type": "signoz",
                    "connection_info": {"api_endpoint": "https://test2.signoz.cloud"},
                    "auth_config": {"auth_type": "api_key", "api_key": "key2"}
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_wrong_source_type(self):
        """Test validation fails for unsupported source types."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "cloudwatch",
                    "connection_info": {"region": "us-east-1"},
                    "auth_config": {"auth_type": "api_key", "api_key": "key"}
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_missing_api_endpoint(self):
        """Test validation fails when api_endpoint is missing."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {},
                    "auth_config": {"auth_type": "api_key", "api_key": "key"}
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_missing_api_key(self):
        """Test validation fails when api_key is missing."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {"api_endpoint": "https://test.signoz.cloud"},
                    "auth_config": {"auth_type": "api_key"}
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_missing_incident_id(self):
        """Test validation fails when incident_id is missing."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {"api_endpoint": "https://test.signoz.cloud"},
                    "auth_config": {"auth_type": "api_key", "api_key": "key"}
                }
            ],
            "incident": {
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_missing_company_id(self):
        """Test validation fails when company_id is missing."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {"api_endpoint": "https://test.signoz.cloud"},
                    "auth_config": {"auth_type": "api_key", "api_key": "key"}
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "title": "Test",
                "service": {"name": "test"}
            }
        }
        
        with pytest.raises(Exception):
            validate_and_extract_payload(payload)
    
    def test_optional_fields(self):
        """Test that optional fields work correctly."""
        payload = {
            "job_type": "incident-data-fetch",
            "data_sources": [
                {
                    "source_type": "signoz",
                    "connection_info": {"api_endpoint": "https://test-signoz.us.signoz.cloud"},
                    "auth_config": {"auth_type": "api_key", "api_key": "key"}
                }
            ],
            "incident": {
                "incident_id": "INC_test_001",
                "company_id": "testcompany",
                "title": "Test",
                "service": {"name": "test"}
                # environment and lookback_hours are optional
            }
        }
        
        incident, endpoint, api_key = validate_and_extract_payload(payload)
        
        # Should not raise an error
        assert incident["incident_id"] == "INC_test_001"
        assert "environment" not in incident or incident.get("environment") is None
        assert "lookback_hours" not in incident or incident.get("lookback_hours") is None

