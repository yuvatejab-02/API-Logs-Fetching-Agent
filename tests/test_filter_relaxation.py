"""Tests for filter relaxation and fallback logic."""
import pytest
from src.llm.filter_relaxation import FilterRelaxation


class TestFilterRelaxation:
    """Test filter relaxation strategies."""
    
    def test_remove_status_code_filter(self):
        """Test removing HTTP status code filters."""
        original = "service.name = 'payments-service' AND http.status_code >= 500 AND http.status_code < 600"
        relaxed, strategy = FilterRelaxation._remove_status_code_filter(original)
        
        assert "http.status_code" not in relaxed
        assert "service.name = 'payments-service'" in relaxed
        assert strategy == "remove_status_code"
    
    def test_keep_service_and_severity_only(self):
        """Test keeping only service and severity filters."""
        original = "service.name = 'api-service' AND severity_text = 'ERROR' AND http.status_code >= 500"
        relaxed, strategy = FilterRelaxation._keep_service_and_severity_only(original)
        
        assert "service.name = 'api-service'" in relaxed
        assert "severity_text = 'ERROR'" in relaxed
        assert "http.status_code" not in relaxed
        assert strategy == "service_and_severity_only"
    
    def test_keep_service_only(self):
        """Test keeping only service filter."""
        original = "service.name = 'payments-service' AND severity_text = 'ERROR' AND http.status_code >= 500"
        relaxed, strategy = FilterRelaxation._keep_service_only(original)
        
        assert relaxed == "service.name = 'payments-service'"
        assert strategy == "service_only"
    
    def test_relax_filter_progression(self):
        """Test filter relaxation progression through attempts."""
        original = "service.name = 'api-service' AND severity_text = 'ERROR' AND http.status_code >= 500"
        
        # Attempt 0: Remove status code
        relaxed_0, strategy_0 = FilterRelaxation.relax_filter(original, 0)
        assert "http.status_code" not in relaxed_0
        assert "service.name" in relaxed_0
        assert strategy_0 == "remove_status_code"
        
        # Attempt 1: Keep service and severity only
        relaxed_1, strategy_1 = FilterRelaxation.relax_filter(original, 1)
        assert "service.name" in relaxed_1
        assert "severity_text" in relaxed_1
        assert "http.status_code" not in relaxed_1
        assert strategy_1 == "service_and_severity_only"
        
        # Attempt 2: Keep service only
        relaxed_2, strategy_2 = FilterRelaxation.relax_filter(original, 2)
        assert relaxed_2 == "service.name = 'api-service'"
        assert strategy_2 == "service_only"
        
        # Attempt 3+: ALL mode (empty filter)
        relaxed_3, strategy_3 = FilterRelaxation.relax_filter(original, 3)
        assert relaxed_3 == ""
        assert strategy_3 == "all_signals_mode"
    
    def test_should_fallback_to_all(self):
        """Test fallback decision logic."""
        # Should not fallback if logs found
        assert not FilterRelaxation.should_fallback_to_all(10, 2, 0)
        
        # Should not fallback if attempts remain
        assert not FilterRelaxation.should_fallback_to_all(0, 2, 0)
        assert not FilterRelaxation.should_fallback_to_all(0, 2, 1)
        
        # Should fallback if no logs and max attempts reached
        assert FilterRelaxation.should_fallback_to_all(0, 2, 2)
        assert FilterRelaxation.should_fallback_to_all(0, 3, 3)
    
    def test_edge_case_no_service_name(self):
        """Test relaxation when no service name is present."""
        original = "http.status_code >= 500 AND severity_text = 'ERROR'"
        
        # Keep service only should return empty when no service
        relaxed, strategy = FilterRelaxation._keep_service_only(original)
        assert relaxed == ""
        assert strategy == "service_only"
    
    def test_edge_case_complex_filter(self):
        """Test relaxation with complex filter expressions."""
        original = "service.name = 'api-service' AND (http.status_code >= 500 OR http.status_code = 429) AND severity_text IN ['ERROR', 'WARN']"
        
        relaxed, strategy = FilterRelaxation._remove_status_code_filter(original)
        assert "http.status_code" not in relaxed
        assert "service.name" in relaxed


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


