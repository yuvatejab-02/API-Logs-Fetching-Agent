"""SigNoz API client for fetching logs."""
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime
from datetime import datetime, timezone

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class SigNozClient:
    """Client for interacting with SigNoz Cloud API."""
    
    def __init__(self):
        """Initialize SigNoz API client."""
        settings = get_settings()
        self.api_endpoint = settings.signoz_api_endpoint.rstrip('/')
        self.api_key = settings.signoz_api_key
        self.timeout = 30
        
        # Set up headers
        self.headers = {
            "Content-Type": "application/json",
            "SIGNOZ-API-KEY": self.api_key
        }
        
        logger.info(
            "signoz_client_initialized",
            endpoint=self.api_endpoint
        )
    
    def fetch_logs(
        self, 
        query_payload: Dict[str, Any],
        incident_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch logs from SigNoz using the provided query.
        
        Args:
            query_payload: The SigNoz API query payload
            incident_id: Optional incident ID for logging context
            
        Returns:
            Raw response from SigNoz API
            
        Raises:
            requests.RequestException: If API call fails
        """
        # Use v5 endpoint for SigNoz Cloud
        url = f"{self.api_endpoint}/api/v5/query_range"
        
        logger.info(
            "fetching_logs_from_signoz",
            incident_id=incident_id,
            url=url,
            time_range=f"{query_payload.get('start')} to {query_payload.get('end')}"
        )
        
        try:
            response = requests.post(
                url=url,
                json=query_payload,
                headers=self.headers,
                timeout=self.timeout
            )
            
            # Log response status
            logger.info(
                "signoz_api_response",
                status_code=response.status_code,
                response_time_ms=response.elapsed.total_seconds() * 1000
            )
            
            # Raise exception for bad status codes
            response.raise_for_status()
            
            # Parse response
            response_data = response.json()
            
            # Extract log count
            log_count = self._extract_log_count(response_data)
            
            logger.info(
                "logs_fetched_successfully",
                incident_id=incident_id,
                log_count=log_count
            )
            
            return response_data
            
        except requests.exceptions.Timeout:
            logger.error(
                "signoz_api_timeout",
                incident_id=incident_id,
                timeout=self.timeout
            )
            raise Exception(f"SigNoz API request timed out after {self.timeout}s")
            
        except requests.exceptions.HTTPError as e:
            logger.error(
                "signoz_api_http_error",
                incident_id=incident_id,
                status_code=e.response.status_code,
                response_body=e.response.text[:500]
            )
            raise Exception(f"SigNoz API error ({e.response.status_code}): {e.response.text}")
            
        except requests.exceptions.RequestException as e:
            logger.error(
                "signoz_api_request_failed",
                incident_id=incident_id,
                error=str(e)
            )
            raise Exception(f"Failed to fetch logs from SigNoz: {str(e)}")
    
    def _extract_log_count(self, response_data: Dict[str, Any]) -> int:
        """Extract the number of logs from SigNoz v5 response.
        
        Args:
            response_data: The SigNoz API response
            
        Returns:
            Number of logs fetched
        """
        try:
            # SigNoz v5 response structure: data.data.results[0].rows
            results = response_data.get('data', {}).get('data', {}).get('results', [])
            if results and len(results) > 0:
                rows = results[0].get('rows', [])
                return len(rows) if rows else 0
            return 0
        except (KeyError, IndexError, TypeError):
            return 0
    
    def test_connection(self) -> bool:
        """Test connection to SigNoz API.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try a simple query to check connectivity
            test_query = {
                "start": int((datetime.now(timezone.utc).timestamp() - 3600) * 1000),
                "end": int(datetime.now(timezone.utc).timestamp() * 1000),
                "requestType": "raw",
                "compositeQuery": {
                    "queries": [{
                        "type": "builder_query",
                        "spec": {
                            "name": "A",
                            "signal": "logs",
                            "filter": {"expression": ""},
                            "limit": 1
                        }
                    }]
                }
            }
            
            response = requests.post(
                url=f"{self.api_endpoint}/api/v5/query_range",
                json=test_query,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("signoz_connection_test_passed")
                return True
            else:
                logger.warning(
                    "signoz_connection_test_failed",
                    status_code=response.status_code
                )
                return False
                
        except Exception as e:
            logger.error("signoz_connection_test_error", error=str(e))
            return False
