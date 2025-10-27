"""Transform SigNoz logs to expected format."""
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from ..utils.logger import get_logger

logger = get_logger(__name__)


class LogTransformer:
    """Transform SigNoz log data to expected flat format."""
    
    @staticmethod
    def transform_logs(signoz_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform SigNoz v5 response to expected log format.
        
        Args:
            signoz_response: Raw response from SigNoz API v5
            
        Returns:
            List of transformed log entries in expected format
        """
        transformed_logs = []
        
        try:
            # Navigate SigNoz v5 response structure: data.data.results[0].rows
            results = signoz_response.get('data', {}).get('data', {}).get('results', [])
            
            if not results:
                logger.warning("no_results_in_response")
                return []
            
            # Extract logs from rows
            rows = results[0].get('rows', []) if results else []
            
            if not rows:
                logger.warning("no_logs_in_response")
                return []
            
            logger.info(
                "transforming_logs",
                total_logs=len(rows)
            )
            
            for log_entry in rows:
                try:
                    transformed = LogTransformer._transform_single_log(log_entry)
                    if transformed:
                        transformed_logs.append(transformed)
                except Exception as e:
                    logger.warning(
                        "failed_to_transform_log",
                        error=str(e),
                        log_id=log_entry.get('data', {}).get('id')
                    )
                    continue
            
            logger.info(
                "logs_transformed_successfully",
                total_transformed=len(transformed_logs)
            )
            
            return transformed_logs
            
        except Exception as e:
            logger.error(
                "log_transformation_failed",
                error=str(e)
            )
            raise
    
    @staticmethod
    def _transform_single_log(log_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a single log entry to expected format.
        
        Args:
            log_entry: Single log entry from SigNoz v5
            
        Returns:
            Transformed log entry in flat format
        """
        data = log_entry.get('data', {})
        
        # Extract nested attributes
        attrs_string = data.get('attributes_string', {})
        attrs_number = data.get('attributes_number', {})
        resources_string = data.get('resources_string', {})
        
        # Build flat log structure
        transformed = {
            # Timestamp
            "timestamp": log_entry.get('timestamp', ''),
            
            # Service info (from resources)
            "service": resources_string.get('service.name', 'unknown'),
            "instance_id": resources_string.get('service.instance.id', ''),
            
            # Severity
            "level": data.get('severity_text', 'INFO'),
            
            # Request identifiers
            "request_id": attrs_string.get('trace_id', ''),
            
            # Company/user info
            "company_id": resources_string.get('deployment.environment', ''),
            "user_id": attrs_string.get('user_id', ''),
            
            # HTTP details
            "method": attrs_string.get('http.method', ''),
            "path": attrs_string.get('http.route', ''),
            "status_code": attrs_number.get('http.status_code', 0),
            "response_time_ms": attrs_number.get('response_time_ms', 0),
            
            # Error details
            "error_message": attrs_string.get('error_message', ''),
            "stack_trace": attrs_string.get('stack_trace', ''),
            
            # Log message
            "message": data.get('body', ''),
            
            # Additional metadata
            # "log_id": data.get('id', ''),
            # "scope_name": data.get('scope_name', ''),
        }
        
        # Remove empty fields to keep logs clean
        transformed = {k: v for k, v in transformed.items() if v not in [None, '', 0, {}]}
        
        return transformed
    
    @staticmethod
    def _format_timestamp(timestamp: Optional[str]) -> str:
        """Format timestamp to ISO format.
        
        Args:
            timestamp: Timestamp string from SigNoz
            
        Returns:
            ISO formatted timestamp string
        """
        if not timestamp:
            return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        try:
            # SigNoz returns ISO format, just ensure Z suffix
            if timestamp.endswith('Z'):
                return timestamp
            return timestamp + 'Z'
        except Exception:
            return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
