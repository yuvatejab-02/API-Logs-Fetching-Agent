"""Tests for EDAL datasource descriptor generator."""
import pytest
from unittest.mock import Mock, patch
from src.storage.edal_descriptor import EDALDescriptorGenerator


class TestEDALDescriptorGenerator:
    """Test EDAL descriptor generation."""
    
    @pytest.fixture
    def generator(self):
        """Create EDALDescriptorGenerator instance."""
        return EDALDescriptorGenerator()
    
    def test_generate_descriptor_with_iam_role(self, generator):
        """Test generating descriptor with IAM role auth."""
        descriptor = generator.generate_descriptor(
            company_id="bugraid",
            tenant="bugraid_prod",
            service="payments",
            environment="prod",
            signals=["logs", "traces"],
            use_iam_role=True,
            role_arn="arn:aws:iam::123456789012:role/edal-reader"
        )
        
        assert descriptor['company_id'] == "bugraid"
        assert len(descriptor['data_sources']) == 2
        assert 'created_at' in descriptor
        assert 'updated_at' in descriptor
        assert descriptor['version'] == "1.0"
        
        # Check logs data source
        logs_source = next(s for s in descriptor['data_sources'] if s['metadata']['signal'] == 'logs')
        assert logs_source['source_id'] == "s3-prod-logs"
        assert logs_source['source_type'] == "s3"
        assert logs_source['connection_info']['path_prefix'] == "raw/prod/bugraid_prod/payments/logs"
        assert logs_source['auth_config']['auth_type'] == "iam_role"
        assert logs_source['auth_config']['role_arn'] == "arn:aws:iam::123456789012:role/edal-reader"
    
    def test_generate_descriptor_with_access_key(self, generator):
        """Test generating descriptor with access key auth (for reference)."""
        descriptor = generator.generate_descriptor(
            company_id="testco",
            tenant="testco_prod",
            service="api",
            environment="prod",
            signals=["logs"],
            use_iam_role=False
        )
        
        logs_source = descriptor['data_sources'][0]
        assert logs_source['auth_config']['auth_type'] == "access_key"
        assert "${SECRET:" in logs_source['auth_config']['access_key_id']
        assert "${SECRET:" in logs_source['auth_config']['secret_access_key']
    
    def test_generate_descriptor_default_signals(self, generator):
        """Test descriptor generation with default signals."""
        descriptor = generator.generate_descriptor(
            company_id="testco",
            tenant="testco",
            service="api",
            use_iam_role=True,
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        
        # Default signals should be logs and traces
        signals = [s['metadata']['signal'] for s in descriptor['data_sources']]
        assert "logs" in signals
        assert "traces" in signals
        assert len(signals) == 2
    
    def test_generate_descriptor_custom_signals(self, generator):
        """Test descriptor with custom signals list."""
        descriptor = generator.generate_descriptor(
            company_id="testco",
            tenant="testco",
            service="api",
            signals=["logs", "metrics", "traces"],
            use_iam_role=True,
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        
        assert len(descriptor['data_sources']) == 3
        signals = [s['metadata']['signal'] for s in descriptor['data_sources']]
        assert "logs" in signals
        assert "metrics" in signals
        assert "traces" in signals
    
    def test_descriptor_metadata(self, generator):
        """Test metadata in data sources."""
        descriptor = generator.generate_descriptor(
            company_id="testco",
            tenant="testco",
            service="payments",
            environment="stage",
            signals=["logs"],
            use_iam_role=True,
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        
        source = descriptor['data_sources'][0]
        metadata = source['metadata']
        
        assert metadata['signal'] == "logs"
        assert metadata['environment'] == "stage"
        assert metadata['service'] == "payments"
        assert metadata['tenant'] == "testco"
        assert metadata['format'] == "json.gz"
        assert metadata['schema_version'] == "v1"
    
    def test_save_descriptor_to_s3(self, generator):
        """Test saving descriptor to S3."""
        mock_s3_client = Mock()
        descriptor = {
            "company_id": "testco",
            "data_sources": [],
            "version": "1.0"
        }
        
        s3_key = generator.save_descriptor_to_s3(
            descriptor=descriptor,
            s3_client=mock_s3_client,
            bucket_name="test-bucket",
            tenant="testco",
            environment="prod"
        )
        
        # Verify S3 upload was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args[1]
        
        assert call_args['Bucket'] == "test-bucket"
        assert "config/prod/testco/edal/datasources_" in call_args['Key']
        assert ".json" in call_args['Key']
        assert call_args['ContentType'] == 'application/json'
        assert call_args['Metadata']['type'] == 'edal_descriptor'
        assert call_args['Metadata']['tenant'] == 'testco'
    
    def test_generate_and_save(self, generator):
        """Test combined generate and save operation."""
        mock_s3_client = Mock()
        
        descriptor, s3_key = generator.generate_and_save(
            company_id="testco",
            tenant="testco",
            service="api",
            s3_client=mock_s3_client,
            environment="prod",
            signals=["logs"],
            use_iam_role=True,
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        
        assert descriptor['company_id'] == "testco"
        assert len(descriptor['data_sources']) == 1
        assert "config/prod/testco/edal/" in s3_key
        mock_s3_client.put_object.assert_called_once()
    
    def test_missing_role_arn_error(self, generator):
        """Test error when IAM role is requested but ARN not provided."""
        with pytest.raises(ValueError) as exc_info:
            generator.generate_descriptor(
                company_id="testco",
                tenant="testco",
                service="api",
                use_iam_role=True,
                role_arn=None  # Missing ARN
            )
        
        assert "role_arn is required" in str(exc_info.value)
    
    def test_path_prefix_structure(self, generator):
        """Test that path prefixes follow correct structure."""
        descriptor = generator.generate_descriptor(
            company_id="testco",
            tenant="tenant_prod",
            service="payments",
            environment="stage",
            signals=["logs", "traces"],
            use_iam_role=True,
            role_arn="arn:aws:iam::123456789012:role/test"
        )
        
        for source in descriptor['data_sources']:
            signal = source['metadata']['signal']
            expected_prefix = f"raw/stage/tenant_prod/payments/{signal}"
            assert source['connection_info']['path_prefix'] == expected_prefix


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


