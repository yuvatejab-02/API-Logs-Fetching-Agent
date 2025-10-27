#!/usr/bin/env python3
"""
Main Production Script - Incident Log Analyzer
Handles: Payload → LLM Query → Historical + Real-time Polling → S3 Upload
"""
import sys
import json
import gzip
from typing import Dict, Any
from pathlib import Path
from datetime import datetime, timezone

from .utils.logger import setup_logging, get_logger
from .utils.config import get_settings
from .polling.incident_poller import IncidentPoller

setup_logging()
logger = get_logger(__name__)


class IncidentLogAnalyzer:
    """
    Main orchestrator for incident log analysis.
    
    Workflow:
    1. Receive incident payload
    2. Generate SigNoz query via LLM (Bedrock Claude)
    3. Fetch historical logs (initial lookback)
    4. Poll for real-time logs continuously
    5. Aggregate and compress data
    6. Upload to S3 bucket
    """
    
    def __init__(self):
        """Initialize the analyzer with all components."""
        self.settings = get_settings()
        self.poller = IncidentPoller()
        
        logger.info(
            "incident_log_analyzer_initialized",
            polling_duration_minutes=self.settings.polling_duration_minutes,
            polling_interval_seconds=self.settings.polling_interval_seconds,
            storage_backend="localstack" if self.settings.is_local_environment else "aws_s3",
            s3_bucket=self.settings.s3_bucket_name
        )
    
    def process_incident(
        self,
        incident_payload: Dict[str, Any],
        initial_lookback_hours: int = 1,
        compress_output: bool = True
    ) -> Dict[str, Any]:
        """
        Process an incident end-to-end.
        
        This is the main production method that:
        1. Validates incident payload
        2. Uses LLM to generate intelligent query
        3. Fetches historical logs (lookback window)
        4. Continuously polls for real-time logs
        5. Aggregates and deduplicates logs
        6. Compresses data (optional)
        7. Uploads to S3
        
        Args:
            incident_payload: Incident data from alerting system
            initial_lookback_hours: Historical lookback window (default: 1 hour)
            compress_output: Whether to gzip compress logs (default: True)
            
        Returns:
            Processing summary with status, metrics, and S3 location
        """
        incident_id = incident_payload.get("incident_id", "unknown")
        
        logger.info(
            "processing_incident_started",
            incident_id=incident_id,
            title=incident_payload.get("title"),
            service=incident_payload.get("service", {}).get("name"),
            lookback_hours=initial_lookback_hours,
            compression_enabled=compress_output
        )
        
        try:
            # Step 1: Start continuous polling (includes LLM query generation)
            logger.info(
                "starting_polling_phase",
                incident_id=incident_id,
                duration_minutes=self.settings.polling_duration_minutes
            )
            
            polling_result = self.poller.start_polling(
                incident_payload=incident_payload,
                initial_lookback_hours=initial_lookback_hours
            )
            
            # Step 2: Compress logs if enabled
            compressed_data = None
            if compress_output and polling_result.get("all_logs"):
                logger.info(
                    "compressing_logs",
                    incident_id=incident_id,
                    log_count=len(polling_result["all_logs"])
                )
                compressed_data = self._compress_logs(polling_result["all_logs"])
            
            # Step 3: Create summary
            summary = {
                "status": "completed",
                "incident_id": incident_id,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "polling_summary": {
                    "total_polls": polling_result["total_polls"],
                    "total_logs_fetched": polling_result["total_logs_fetched"],
                    "unique_logs": len(polling_result["all_logs"]),
                    "duplicates_removed": polling_result["total_logs_fetched"] - len(polling_result["all_logs"]),
                    "duration_seconds": int((polling_result["end_time"] - polling_result["start_time"]).total_seconds()),
                    "fetch_history": polling_result.get("fetch_history", [])
                },
                "query_info": {
                    "filter_expression": polling_result["filter_expression"],
                    "llm_model": "anthropic.claude-3-5-sonnet-20241022-v2:0"
                },
                "storage": {
                    "local_path": f"output/{incident_id}/final_aggregated_*.json",
                    "s3_uploaded": True,
                    "compressed": compress_output
                }
            }
            
            logger.info(
                "processing_incident_completed",
                incident_id=incident_id,
                **summary["polling_summary"]
            )
            
            return summary
            
        except KeyboardInterrupt:
            logger.warning(
                "processing_incident_interrupted",
                incident_id=incident_id
            )
            return {
                "status": "interrupted",
                "incident_id": incident_id,
                "message": "Processing interrupted by user"
            }
            
        except Exception as e:
            logger.error(
                "processing_incident_failed",
                incident_id=incident_id,
                error=str(e),
                exc_info=True
            )
            
            return {
                "status": "failed",
                "incident_id": incident_id,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def _compress_logs(self, logs: list) -> bytes:
        """
        Compress logs using gzip for efficient S3 storage.
        
        Args:
            logs: List of log entries
            
        Returns:
            Compressed bytes
        """
        try:
            json_data = json.dumps(logs, ensure_ascii=False)
            compressed = gzip.compress(json_data.encode('utf-8'))
            
            compression_ratio = len(compressed) / len(json_data.encode('utf-8'))
            
            logger.info(
                "logs_compressed",
                original_size_kb=len(json_data) / 1024,
                compressed_size_kb=len(compressed) / 1024,
                compression_ratio=f"{compression_ratio:.2%}"
            )
            
            return compressed
            
        except Exception as e:
            logger.error("compression_failed", error=str(e))
            raise


def main():
    """
    Main entry point for production deployment.
    
    Usage:
        # With incident file
        python -m src.main --incident-file incident.json
        
        # With JSON string (from webhook/queue)
        python -m src.main --incident-json '{"incident_id":"INC_001",...}'
        
        # With environment variables
        export INCIDENT_PAYLOAD='{"incident_id":"INC_001",...}'
        python -m src.main
    """
    import argparse
    import os
    
    parser = argparse.ArgumentParser(
        description="Incident Log Analyzer - Production Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From file
  python -m src.main --incident-file /data/incident.json
  
  # From JSON string
  python -m src.main --incident-json '{"incident_id":"INC_001","service":{"name":"payments"}}'
  
  # With custom lookback
  python -m src.main --incident-file incident.json --lookback-hours 2
  
  # Disable compression
  python -m src.main --incident-file incident.json --no-compress
        """
    )
    
    parser.add_argument(
        "--incident-file",
        type=str,
        help="/tests/test_data/sample_payloads.json"
    )
    parser.add_argument(
        "--incident-json",
        type=str,
        help=""
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=1,
        help="Initial historical lookback window in hours (default: 1)"
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable gzip compression of logs"
    )
    
    args = parser.parse_args()
    
    # Get incident payload from various sources
    incident_payload = None
    
    # Priority 1: Command line file
    if args.incident_file:
        try:
            with open(args.incident_file, 'r') as f:
                incident_payload = json.load(f)
            logger.info("incident_payload_loaded_from_file", file=args.incident_file)
        except Exception as e:
            logger.error("failed_to_load_incident_file", error=str(e))
            sys.exit(1)
    
    # Priority 2: Command line JSON
    elif args.incident_json:
        try:
            incident_payload = json.loads(args.incident_json)
            logger.info("incident_payload_loaded_from_json")
        except Exception as e:
            logger.error("failed_to_parse_incident_json", error=str(e))
            sys.exit(1)
    
    # Priority 3: Environment variable
    elif os.getenv('INCIDENT_PAYLOAD'):
        try:
            incident_payload = json.loads(os.getenv('INCIDENT_PAYLOAD'))
            logger.info("incident_payload_loaded_from_env")
        except Exception as e:
            logger.error("failed_to_parse_env_payload", error=str(e))
            sys.exit(1)
    
    # Priority 4: Default test payload (for testing only)
    else:
        incident_payload = {
            "incident_id": "INC_default_test",
            "title": "Test incident",
            "service": {
                "id": "test_service_id",
                "name": "payments"
            }
        }
        logger.warning("using_default_test_payload")
    
    # Display incident info
    print("\n" + "="*80)
    print("  🚨 INCIDENT LOG ANALYZER - PRODUCTION MODE")
    print("="*80)
    print(f"\nIncident ID: {incident_payload.get('incident_id')}")
    print(f"Title: {incident_payload.get('title', 'N/A')}")
    print(f"Service: {incident_payload.get('service', {}).get('name', 'N/A')}")
    print(f"Lookback: {args.lookback_hours} hour(s)")
    print(f"Compression: {'Enabled' if not args.no_compress else 'Disabled'}")
    print("\n" + "="*80 + "\n")
    
    # Initialize and run analyzer
    analyzer = IncidentLogAnalyzer()
    
    try:
        result = analyzer.process_incident(
            incident_payload=incident_payload,
            initial_lookback_hours=args.lookback_hours,
            compress_output=not args.no_compress
        )
        
        # Print results
        print("\n" + "="*80)
        if result["status"] == "completed":
            print("  ✅ ANALYSIS COMPLETED SUCCESSFULLY")
            print("="*80)
            print(f"\n📊 Summary:")
            print(f"   Total Polls: {result['polling_summary']['total_polls']}")
            print(f"   Logs Fetched: {result['polling_summary']['total_logs_fetched']}")
            print(f"   Unique Logs: {result['polling_summary']['unique_logs']}")
            print(f"   Duration: {result['polling_summary']['duration_seconds']}s")
            print(f"\n🔍 Query Filter:")
            print(f"   {result['query_info']['filter_expression']}")
            print(f"\n💾 Storage:")
            print(f"   S3 Uploaded: {result['storage']['s3_uploaded']}")
            print(f"   Compressed: {result['storage']['compressed']}")
        else:
            print("  ❌ ANALYSIS FAILED")
            print("="*80)
            print(f"\nError: {result.get('error', 'Unknown error')}")
        
        print("\n" + "="*80 + "\n")
        
        sys.exit(0 if result["status"] == "completed" else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user\n")
        sys.exit(130)


if __name__ == "__main__":
    main()
