"""Local file storage for logs during development."""
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from ..utils.logger import get_logger

logger = get_logger(__name__)


class LocalStorage:
    """Handle local file storage for logs."""
    
    def __init__(self, base_dir: str = "output"):
        """Initialize local storage.
        
        Args:
            base_dir: Base directory for storing logs
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            "local_storage_initialized",
            base_dir=str(self.base_dir.absolute())
        )
    
    def save_logs(
        self,
        logs: List[Dict[str, Any]],
        incident_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Save logs to local file.
        
        Args:
            logs: List of log entries
            incident_id: Incident identifier
            metadata: Optional metadata about the fetch
            
        Returns:
            Path to saved file
        """
        # Create incident directory
        incident_dir = self.base_dir / incident_id
        incident_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        # Check if this is a final aggregated result
        is_final = metadata and "polling_summary" in metadata
        prefix = "final_aggregated" if is_final else "logs"
        
        filename = f"{prefix}_{timestamp}.json"
        filepath = incident_dir / filename
        
        # Prepare data to save
        output_data = {
            "metadata": {
                "incident_id": incident_id,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "log_count": len(logs),
                "is_final_aggregated": is_final,
                **(metadata or {})
            },
            "logs": logs
        }
        
        # Write to file
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(
                "logs_saved_locally",
                incident_id=incident_id,
                filepath=str(filepath),
                log_count=len(logs),
                is_final=is_final
            )
            
            return str(filepath)
            
        except Exception as e:
            logger.error(
                "failed_to_save_logs",
                incident_id=incident_id,
                error=str(e)
            )
            raise
    
    def save_raw_response(
        self,
        raw_response: Dict[str, Any],
        incident_id: str
    ) -> str:
        """Save raw SigNoz response for debugging.
        
        Args:
            raw_response: Raw response from SigNoz
            incident_id: Incident identifier
            
        Returns:
            Path to saved file
        """
        incident_dir = self.base_dir / incident_id / "raw"
        incident_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"raw_response_{timestamp}.json"
        filepath = incident_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(raw_response, f, indent=2, ensure_ascii=False)
            
            logger.debug(
                "raw_response_saved",
                filepath=str(filepath)
            )
            
            return str(filepath)
            
        except Exception as e:
            logger.warning(
                "failed_to_save_raw_response",
                error=str(e)
            )
            return ""
