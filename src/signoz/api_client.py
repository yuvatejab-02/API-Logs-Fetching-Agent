"""SigNoz API client for dry-run queries."""
import requests
from typing import Dict, Any, Optional

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class SigNozClient:
    """Lightweight client for SigNoz API dry-run queries."""
    
    def __init__(self):
        """Initialize SigNoz API client."""
        settings = get_settings()
        self.api_endpoint = settings.signoz_api_endpoint.rstrip('/')
        self.api_key = settings.signoz_api_key
        self.timeout = 30
        self.headers = {
            "Content-Type": "application/json",
            "SIGNOZ-API-KEY": self.api_key
        }
        logger.info("signoz_client_initialized", endpoint=self.api_endpoint)
    
    def dry_run_query(
        self,
        filter_expression: str,
        start_ms: int,
        end_ms: int,
        limit: int = 10,
        incident_id: Optional[str] = None
    ) -> int:
        """Execute a dry-run query to check if filter returns results.
        
        Args:
            filter_expression: SigNoz filter expression to test
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            limit: Maximum logs to fetch for testing (default: 10)
            incident_id: Optional incident ID for logging
            
        Returns:
            Number of logs found (0 if no results or error)
        """
        query_payload = {
            "start": start_ms,
            "end": end_ms,
            "requestType": "raw",
            "variables": {},
            "compositeQuery": {
                "queries": [{
                    "type": "builder_query",
                    "spec": {
                        "name": "A",
                        "signal": "logs",
                        "filter": {"expression": filter_expression},
                        "order": [
                            {"key": {"name": "timestamp"}, "direction": "desc"},
                            {"key": {"name": "id"}, "direction": "desc"}
                        ],
                        "offset": 0,
                        "limit": limit
                    }
                }]
            }
        }
        
        logger.info(
            "executing_dry_run_query",
            incident_id=incident_id,
            filter_expression=filter_expression,
            limit=limit
        )
        
        try:
            response = requests.post(
                url=f"{self.api_endpoint}/api/v5/query_range",
                json=query_payload,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            response_data = response.json()
            
            # Extract log count from response
            log_count = self._extract_count(response_data)
            
            logger.info(
                "dry_run_query_completed",
                incident_id=incident_id,
                log_count=log_count,
                filter_expression=filter_expression
            )
            
            return log_count
            
        except Exception as e:
            logger.error(
                "dry_run_query_failed",
                incident_id=incident_id,
                error=str(e),
                filter_expression=filter_expression
            )
            return 0  # Return 0 on error to trigger fallback
    
    def _extract_count(self, response_data: Dict[str, Any]) -> int:
        """Extract row count from SigNoz v5 response."""
        try:
            results = response_data.get('data', {}).get('data', {}).get('results', [])
            if results and len(results) > 0:
                rows = results[0].get('rows', [])
                return len(rows) if rows else 0
            return 0
        except (KeyError, IndexError, TypeError):
            return 0

