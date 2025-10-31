"""Integration test for the new main.py workflow."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.main import IncidentLogAnalyzer


class TestMainIntegration:
    """Test the integrated main workflow."""
    
    @pytest.fixture
    def mock_components(self):
        """Mock all external components."""
        with patch('src.main.QueryGenerator') as mock_qg, \
             patch('src.main.SigNozClient') as mock_sc, \
             patch('src.main.SigNozFetcher') as mock_sf, \
             patch('src.main.RawS3Storage') as mock_s3, \
             patch('src.main.EDALDescriptorGenerator') as mock_edal, \
             patch('src.main.LocalStorage') as mock_local:
            
            # Mock query generator
            mock_qg_instance = Mock()
            mock_qg_instance.generate_signoz_query.return_value = {
                'filters': {
                    'logs': 'service.name = "test" AND severity_text = "ERROR"',
                    'traces': 'resource.service.name = "test"',
                    'metrics': None
                },
                'metadata': {
                    'original_filters': {
                        'logs': 'service.name = "test" AND severity_text = "ERROR"',
                        'traces': 'resource.service.name = "test"',
                        'metrics': None
                    },
                    'fetch_mode': 'FILTERED',
                    'relaxation_history': [],
                    'time_window': {
                        'start_ms': 1234567890000,
                        'end_ms': 1234571490000,
                        'lookback_hours': 1
                    }
                }
            }
            mock_qg.return_value = mock_qg_instance
            
            # Mock SigNoz client
            mock_sc_instance = Mock()
            mock_sc.return_value = mock_sc_instance
            
            # Mock fetcher
            mock_sf_instance = Mock()
            mock_sf_instance.fetch_logs.return_value = {
                'data': {'data': {'results': [{'rows': [{'id': '1'}]}]}}
            }
            mock_sf_instance.fetch_traces.return_value = {
                'data': {'data': {'results': [{'rows': [{'id': '2'}]}]}}
            }
            mock_sf_instance.fetch_traces_paginated.return_value = {
                'data': {'data': {'results': [{'rows': [{'id': '2'}]}]}}
            }
            mock_sf_instance.fetch_metrics.return_value = {
                'data': {'data': {'results': [{'series': [{'name': 'metric1'}]}]}}
            }
            # Mock concurrent fetching method
            mock_sf_instance.fetch_all_signals_concurrent.return_value = {
                'logs': {'data': {'data': {'results': [{'rows': [{'id': '1'}]}]}}},
                'traces': {'data': {'data': {'results': [{'rows': [{'id': '2'}]}]}}},
                'metrics': {'data': {'data': {'results': [{'series': [{'name': 'metric1'}]}]}}}
            }
            mock_sf.return_value = mock_sf_instance
            
            # Mock S3 storage
            mock_s3_instance = Mock()
            mock_s3_instance.upload_raw_signal.return_value = 's3://bucket/key'
            mock_s3_instance.upload_manifest.return_value = 's3://bucket/manifest'
            mock_s3_instance.s3_client = Mock()
            mock_s3.return_value = mock_s3_instance
            
            # Mock EDAL generator
            mock_edal_instance = Mock()
            mock_edal_instance.generate_and_save.return_value = (
                {'company_id': 'test'},
                's3://bucket/edal'
            )
            mock_edal.return_value = mock_edal_instance
            
            # Mock local storage
            mock_local_instance = Mock()
            mock_local.return_value = mock_local_instance
            
            yield {
                'query_generator': mock_qg_instance,
                'signoz_client': mock_sc_instance,
                'fetcher': mock_sf_instance,
                's3_storage': mock_s3_instance,
                'edal_generator': mock_edal_instance,
                'local_storage': mock_local_instance
            }
    
    def test_process_incident_success(self, mock_components):
        """Test successful incident processing."""
        analyzer = IncidentLogAnalyzer()
        
        incident_payload = {
            'incident_id': 'INC_TEST_001',
            'title': 'Test Incident',
            'service': {'name': 'test-service'}
        }
        
        result = analyzer.process_incident(
            incident_payload=incident_payload,
            initial_lookback_hours=1,
            tenant='test_tenant',
            environment='prod',
            generate_edal=True
        )
        
        # Verify result
        assert result['status'] == 'completed'
        assert result['incident_id'] == 'INC_TEST_001'
        assert 'query_info' in result
        assert 'signals' in result
        assert 'storage' in result
        
        # Verify components were called
        mock_components['query_generator'].generate_signoz_query.assert_called_once()
        mock_components['fetcher'].fetch_all_signals_concurrent.assert_called_once()
        mock_components['s3_storage'].upload_raw_signal.assert_called()
        mock_components['edal_generator'].generate_and_save.assert_called_once()
    
    def test_process_incident_all_mode(self, mock_components):
        """Test incident processing in ALL mode."""
        # Configure for ALL mode
        mock_components['query_generator'].generate_signoz_query.return_value = {
            'filters': {
                'logs': '',
                'traces': '',
                'metrics': None
            },
            'metadata': {
                'original_filters': {
                    'logs': 'service.name = "test"',
                    'traces': 'resource.service.name = "test"',
                    'metrics': None
                },
                'fetch_mode': 'ALL_SIGNALS',
                'relaxation_history': [
                    {'attempt': 1, 'strategy': 'remove_status_code'},
                    {'attempt': 2, 'strategy': 'service_only'}
                ],
                'time_window': {
                    'start_ms': 1234567890000,
                    'end_ms': 1234571490000,
                    'lookback_hours': 1
                }
            }
        }
        
        analyzer = IncidentLogAnalyzer()
        
        incident_payload = {
            'incident_id': 'INC_TEST_002',
            'service': {'name': 'test-service'}
        }
        
        result = analyzer.process_incident(
            incident_payload=incident_payload,
            tenant='test_tenant'
        )
        
        assert result['status'] == 'completed'
        assert result['query_info']['fetch_mode'] == 'ALL_SIGNALS'
        assert len(result['query_info']['relaxation_history']) == 2
        # In ALL mode, we use concurrent fetching with empty filters
        mock_components['fetcher'].fetch_all_signals_concurrent.assert_called_once()
    
    def test_process_incident_without_edal(self, mock_components):
        """Test incident processing without EDAL generation."""
        analyzer = IncidentLogAnalyzer()
        
        incident_payload = {
            'incident_id': 'INC_TEST_003',
            'service': {'name': 'test-service'}
        }
        
        result = analyzer.process_incident(
            incident_payload=incident_payload,
            generate_edal=False
        )
        
        assert result['status'] == 'completed'
        assert result['storage']['edal_descriptor_key'] is None
        mock_components['edal_generator'].generate_and_save.assert_not_called()
    
    def test_process_incident_error_handling(self, mock_components):
        """Test error handling in incident processing."""
        # Make concurrent fetcher return an error for logs but success for others
        mock_components['fetcher'].fetch_all_signals_concurrent.return_value = {
            'logs': {'error': 'API Error'},
            'traces': {'data': {'data': {'results': [{'rows': [{'id': '2'}]}]}}},
            'metrics': {'data': {'data': {'results': [{'series': [{'name': 'metric1'}]}]}}}
        }
        
        analyzer = IncidentLogAnalyzer()
        
        incident_payload = {
            'incident_id': 'INC_TEST_004',
            'service': {'name': 'test-service'}
        }
        
        result = analyzer.process_incident(incident_payload=incident_payload)
        
        # Should handle error gracefully and continue with other signals
        # (logs failed but traces and metrics succeeded, so status is completed)
        assert result['status'] == 'completed'
        # Verify traces and metrics were still uploaded
        mock_components['s3_storage'].upload_raw_signal.assert_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

