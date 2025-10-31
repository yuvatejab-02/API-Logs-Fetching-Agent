"""Tests for raw S3 storage with partitioned structure."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from src.storage.raw_s3_storage import RawS3Storage


class TestRawS3Storage:
    """Test raw S3 storage functionality."""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()
    
    @pytest.fixture
    def storage(self, mock_s3_client):
        """Create RawS3Storage instance with mocked S3 client."""
        with patch('src.storage.raw_s3_storage.boto3.client', return_value=mock_s3_client):
            storage = RawS3Storage()
            storage.s3_client = mock_s3_client
            return storage
    
    def test_build_key_structure(self, storage):
        """Test S3 key structure with partitions."""
        end_ms = int(datetime(2025, 10, 30, 15, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        
        key = storage._build_key(
            environment="prod",
            tenant="bugraid",
            service="payments",
            signal="logs",
            end_ms=end_ms,
            incident_id="INC_001",
            start_ms=end_ms - 3600000,
            sequence=1,
            part=1
        )
        
        # Verify key structure
        assert key.startswith("raw/prod/bugraid/payments/logs/")
        assert "date=2025-10-30" in key
        assert "hour=15" in key
        assert "incident_id=INC_001" in key
        assert "seq=00001" in key
        assert "part-0001.json.gz" in key
    
    def test_compress_json(self, storage):
        """Test JSON compression."""
        data = {
            "logs": [
                {"id": "1", "message": "test log 1"},
                {"id": "2", "message": "test log 2"}
            ]
        }
        
        compressed, original_size, compressed_size = storage._compress_json(data)
        
        assert isinstance(compressed, bytes)
        assert original_size > 0
        assert compressed_size > 0
        assert compressed_size < original_size  # Compression should reduce size
    
    def test_upload_raw_signal(self, storage, mock_s3_client):
        """Test uploading raw signal data."""
        data = {"data": {"results": [{"rows": [{"id": "1"}]}]}}
        
        s3_key = storage.upload_raw_signal(
            data=data,
            signal="logs",
            incident_id="INC_001",
            start_ms=1000000,
            end_ms=2000000,
            sequence=1,
            part=1,
            tenant="test_tenant",
            service="test_service",
            environment="prod"
        )
        
        # Verify S3 client was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]
        
        assert call_args['ContentType'] == 'application/json'
        assert call_args['ContentEncoding'] == 'gzip'
        assert 'Metadata' in call_args
        assert call_args['Metadata']['signal'] == 'logs'
        assert call_args['Metadata']['incident_id'] == 'INC_001'
        assert s3_key.startswith("raw/prod/test_tenant/test_service/logs/")
    
    def test_upload_manifest(self, storage, mock_s3_client):
        """Test uploading manifest file."""
        manifest_data = {
            "incident_id": "INC_001",
            "signal": "logs",
            "total_uploads": 5,
            "data_completeness": "complete"
        }
        
        s3_key = storage.upload_manifest(
            incident_id="INC_001",
            signal="logs",
            manifest_data=manifest_data,
            tenant="test_tenant",
            service="test_service",
            environment="prod"
        )
        
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]
        
        assert call_args['ContentType'] == 'application/json'
        assert 'manifest.json' in s3_key
        assert 'incident_id=INC_001' in s3_key
    
    def test_upload_with_metadata(self, storage, mock_s3_client):
        """Test uploading with custom metadata."""
        data = {"test": "data"}
        custom_metadata = {
            "fetch_mode": "ALL_SIGNALS",
            "filter_expression": "service.name = 'api'"
        }
        
        storage.upload_raw_signal(
            data=data,
            signal="logs",
            incident_id="INC_001",
            start_ms=1000000,
            end_ms=2000000,
            sequence=1,
            tenant="test",
            service="api",
            metadata=custom_metadata
        )
        
        call_args = mock_s3_client.put_object.call_args[1]
        metadata = call_args['Metadata']
        
        assert 'custom_fetch_mode' in metadata
        assert metadata['custom_fetch_mode'] == 'ALL_SIGNALS'
    
    def test_list_incident_data(self, storage, mock_s3_client):
        """Test listing incident data."""
        # Mock paginator
        mock_paginator = Mock()
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'raw/prod/tenant/service/logs/date=2025-10-30/hour=15/incident_id=INC_001/window=1-2/seq=00001/part-0001.json.gz'},
                    {'Key': 'raw/prod/tenant/service/logs/date=2025-10-30/hour=15/incident_id=INC_001/window=1-2/seq=00002/part-0001.json.gz'}
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        keys = storage.list_incident_data(
            incident_id="INC_001",
            signal="logs",
            tenant="tenant",
            service="service",
            environment="prod"
        )
        
        assert len(keys) == 2
        assert all("incident_id=INC_001" in key for key in keys)
    
    def test_upload_error_handling(self, storage, mock_s3_client):
        """Test error handling during upload."""
        from botocore.exceptions import ClientError
        
        error_response = {'Error': {'Code': '500', 'Message': 'Internal Error'}}
        mock_s3_client.put_object.side_effect = ClientError(error_response, 'PutObject')
        
        with pytest.raises(Exception) as exc_info:
            storage.upload_raw_signal(
                data={"test": "data"},
                signal="logs",
                incident_id="INC_001",
                start_ms=1000000,
                end_ms=2000000,
                sequence=1
            )
        
        assert "Failed to upload" in str(exc_info.value)
    
    def test_key_partitioning_by_time(self, storage):
        """Test that keys are correctly partitioned by date and hour."""
        # Test different times
        times = [
            datetime(2025, 10, 30, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 10, 30, 15, 30, 0, tzinfo=timezone.utc),
            datetime(2025, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
        ]
        
        for dt in times:
            end_ms = int(dt.timestamp() * 1000)
            key = storage._build_key(
                environment="prod",
                tenant="test",
                service="api",
                signal="logs",
                end_ms=end_ms,
                incident_id="INC_001",
                start_ms=end_ms - 3600000,
                sequence=1,
                part=1
            )
            
            expected_date = dt.strftime("%Y-%m-%d")
            expected_hour = dt.strftime("%H")
            
            assert f"date={expected_date}" in key
            assert f"hour={expected_hour}" in key


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


