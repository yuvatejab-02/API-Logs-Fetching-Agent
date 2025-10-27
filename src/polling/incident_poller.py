"""Continuous polling system for incident logs."""
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from ..storage.s3_storage import S3Storage

from ..utils.config import get_settings
from ..utils.logger import get_logger
from ..llm.query_generator import QueryGenerator
from ..signoz.api_client import SigNozClient
from ..signoz.log_transformer import LogTransformer
from ..storage.local_storage import LocalStorage

logger = get_logger(__name__)


class IncidentPoller:
    """Continuously polls for NEW logs during an incident (incremental fetching)."""
    
    def __init__(self):
        """Initialize the incident poller."""
        self.settings = get_settings()
        self.query_generator = QueryGenerator()
        self.signoz_client = SigNozClient()
        self.log_transformer = LogTransformer()
        self.local_storage = LocalStorage()
        
        self.polling_duration = self.settings.polling_duration_minutes * 60  # Convert to seconds
        self.polling_interval = self.settings.polling_interval_seconds
        
        
        self.s3_storage = S3Storage()
        
        logger.info(
            "incident_poller_initialized",
            polling_duration_minutes=self.settings.polling_duration_minutes,
            polling_interval_seconds=self.polling_interval
        )
    
    def start_polling(
        self,
        incident_payload: Dict[str, Any],
        initial_lookback_hours: int = 1
    ) -> Dict[str, Any]:
        """Start continuous polling for an incident with incremental fetching.
        
        Args:
            incident_payload: The incident data
            initial_lookback_hours: Initial lookback window for first poll (default: 1 hour)
            
        Returns:
            Summary of polling results
        """
        incident_id = incident_payload.get("incident_id", "unknown")
        
        logger.info(
            "starting_incident_polling",
            incident_id=incident_id,
            duration_minutes=self.settings.polling_duration_minutes,
            interval_seconds=self.polling_interval,
            initial_lookback_hours=initial_lookback_hours
        )
        
        # Generate filter expression from LLM
        query_result = self.query_generator.generate_signoz_query(
            incident_payload=incident_payload,
            lookback_hours=initial_lookback_hours
        )
        
        filter_expression = query_result['metadata']['filter_expression']
        
        logger.info(
            "filter_expression_generated",
            incident_id=incident_id,
            filter_expression=filter_expression
        )
        
        # Track polling state
        polling_state = {
            "incident_id": incident_id,
            "filter_expression": filter_expression,
            "start_time": datetime.now(timezone.utc),
            "end_time": None,
            "last_fetch_timestamp": None,  # Track last successful fetch time
            "total_polls": 0,
            "total_logs_fetched": 0,
            "all_logs": [],
            "fetch_history": []
        }
        
        # Calculate polling end time
        polling_end_time = datetime.now(timezone.utc) + timedelta(
            minutes=self.settings.polling_duration_minutes
        )
        
        poll_count = 0
        
        try:
            while datetime.now(timezone.utc) < polling_end_time:
                poll_count += 1
                current_time = datetime.now(timezone.utc)
                
                # Determine time window for this poll
                if poll_count == 1:
                    # First poll: fetch historical data (initial lookback)
                    start_time = current_time - timedelta(hours=initial_lookback_hours)
                    end_time = current_time
                    is_initial_poll = True
                else:
                    # Subsequent polls: fetch only NEW data since last poll
                    start_time = polling_state["last_fetch_timestamp"]
                    end_time = current_time
                    is_initial_poll = False
                
                logger.info(
                    "starting_poll",
                    incident_id=incident_id,
                    poll_number=poll_count,
                    is_initial_poll=is_initial_poll,
                    time_window_start=start_time.isoformat(),
                    time_window_end=end_time.isoformat(),
                    remaining_time_seconds=int((polling_end_time - current_time).total_seconds())
                )
                
                # Perform poll
                poll_result = self._perform_poll(
                    incident_id=incident_id,
                    filter_expression=filter_expression,
                    start_time=start_time,
                    end_time=end_time
                )
                
                # Update last fetch timestamp to current time
                polling_state["last_fetch_timestamp"] = end_time
                
                # Update state
                polling_state["total_polls"] = poll_count
                polling_state["total_logs_fetched"] += poll_result["log_count"]
                polling_state["all_logs"].extend(poll_result["logs"])
                polling_state["fetch_history"].append({
                    "poll_number": poll_count,
                    "timestamp": current_time.isoformat(),
                    "log_count": poll_result["log_count"],
                    "time_range": poll_result["time_range"],
                    "is_initial_poll": is_initial_poll
                })
                
                logger.info(
                    "poll_completed",
                    incident_id=incident_id,
                    poll_number=poll_count,
                    logs_fetched=poll_result["log_count"],
                    total_logs=polling_state["total_logs_fetched"]
                )
                
                # Save intermediate results if logs were fetched
                if poll_result["log_count"] > 0:
                    self._save_poll_results(
                        incident_id=incident_id,
                        poll_number=poll_count,
                        logs=poll_result["logs"],
                        metadata={
                            **query_result['metadata'],
                            "poll_number": poll_count,
                            "is_initial_poll": is_initial_poll,
                            "time_window": poll_result["time_range"]
                        }
                    )
                
                # Wait before next poll (unless it's the last one)
                if datetime.now(timezone.utc) < polling_end_time:
                    logger.info(
                        "waiting_for_next_poll",
                        wait_seconds=self.polling_interval
                    )
                    time.sleep(self.polling_interval)
            
            # Polling complete
            polling_state["end_time"] = datetime.now(timezone.utc)
            
            logger.info(
                "polling_completed",
                incident_id=incident_id,
                total_polls=poll_count,
                total_logs=polling_state["total_logs_fetched"],
                duration_seconds=int((polling_state["end_time"] - polling_state["start_time"]).total_seconds())
            )
            
            # Save final aggregated results
            self._save_final_results(polling_state, query_result['metadata'])
            
            return polling_state
            
        except KeyboardInterrupt:
            logger.warning(
                "polling_interrupted_by_user",
                incident_id=incident_id,
                polls_completed=poll_count
            )
            polling_state["end_time"] = datetime.now(timezone.utc)
            self._save_final_results(polling_state, query_result['metadata'])
            return polling_state
            
        except Exception as e:
            logger.error(
                "polling_failed",
                incident_id=incident_id,
                error=str(e),
                polls_completed=poll_count,
                exc_info=True
            )
            raise
    
    def _perform_poll(
        self,
        incident_id: str,
        filter_expression: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Perform a single poll for logs in the specified time window.
        
        Args:
            incident_id: The incident identifier
            filter_expression: SigNoz filter expression
            start_time: Start of time window (datetime object)
            end_time: End of time window (datetime object)
            
        Returns:
            Poll results with logs and metadata
        """
        # Convert datetime to epoch milliseconds
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        
        # Build SigNoz query
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
                        "limit": 1000
                    }
                }]
            }
        }
        
        logger.debug(
            "executing_poll_query",
            incident_id=incident_id,
            start_ms=start_ms,
            end_ms=end_ms,
            filter=filter_expression
        )
        
        # Fetch logs from SigNoz
        raw_response = self.signoz_client.fetch_logs(
            query_payload=query_payload,
            incident_id=incident_id
        )
        
        # Transform logs
        logs = self.log_transformer.transform_logs(raw_response)
        
        return {
            "log_count": len(logs),
            "logs": logs,
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "start_ms": start_ms,
                "end_ms": end_ms
            }
        }
    
    def _save_poll_results(
        self,
        incident_id: str,
        poll_number: int,
        logs: List[Dict[str, Any]],
        metadata: Dict[str, Any]
    ) -> None:
        """Save results from a single poll.
        
        Args:
            incident_id: The incident identifier
            poll_number: The poll sequence number
            logs: The fetched logs
            metadata: Query metadata
        """
        try:
            poll_metadata = {
                **metadata,
                "poll_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.local_storage.save_logs(
                logs=logs,
                incident_id=incident_id,
                metadata=poll_metadata
            )
        except Exception as e:
            logger.warning(
                "failed_to_save_poll_results",
                incident_id=incident_id,
                poll_number=poll_number,
                error=str(e)
            )
    
    def _save_final_results(
        self,
        polling_state: Dict[str, Any],
        query_metadata: Dict[str, Any]
    ) -> None:
        """Save final aggregated results with deduplication.
        
        Args:
            polling_state: The complete polling state
            query_metadata: Original query metadata
        """
        incident_id = polling_state["incident_id"]
        
        # Remove duplicates based on log_id
        unique_logs = self._deduplicate_logs(polling_state["all_logs"])
        
        # Sort by timestamp (most recent first)
        unique_logs.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        logger.info(
            "deduplication_complete",
            incident_id=incident_id,
            original_count=len(polling_state["all_logs"]),
            unique_count=len(unique_logs),
            duplicates_removed=len(polling_state["all_logs"]) - len(unique_logs)
        )
        
        # Prepare final metadata
        final_metadata = {
            **query_metadata,
            "polling_summary": {
                "start_time": polling_state["start_time"].isoformat(),
                "end_time": polling_state["end_time"].isoformat(),
                "duration_seconds": int((polling_state["end_time"] - polling_state["start_time"]).total_seconds()),
                "total_polls": polling_state["total_polls"],
                "total_logs_fetched": polling_state["total_logs_fetched"],
                "unique_logs": len(unique_logs),
                "duplicates_removed": polling_state["total_logs_fetched"] - len(unique_logs),
                "fetch_history": polling_state["fetch_history"]
            }
        }
        
        # Save aggregated results
        try:
            filepath = self.local_storage.save_logs(
                logs=unique_logs,
                incident_id=incident_id,
                metadata=final_metadata
            )
            
            logger.info(
                "final_results_saved",
                incident_id=incident_id,
                filepath=filepath,
                unique_logs=len(unique_logs)
            )
        except Exception as e:
            logger.error(
                "failed_to_save_final_results",
                incident_id=incident_id,
                error=str(e)
            )
        try:
            s3_key = self.s3_storage.upload_logs(
            logs=unique_logs,
            incident_id=incident_id,
            metadata=final_metadata,
            file_type="final_aggregated"
            )
            logger.info("final_results_uploaded_to_s3", s3_key=s3_key)
        except Exception as e:
            logger.warning("failed_to_upload_to_s3", error=str(e))
    
    def _deduplicate_logs(self, logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate logs based on log_id.
        
        Args:
            logs: List of log entries
            
        Returns:
            Deduplicated list of logs
        """
        seen_ids = set()
        unique_logs = []
        
        for log in logs:
            log_id = log.get("log_id")
            if log_id and log_id not in seen_ids:
                seen_ids.add(log_id)
                unique_logs.append(log)
            elif not log_id:
                # If no log_id, include it (shouldn't happen but handle gracefully)
                unique_logs.append(log)
        
        return unique_logs
