#!/usr/bin/env python3
"""
Main Production Script - Incident Log Analyzer
Handles: SQS → Payload → LLM Query → Multi-Signal Fetch → Raw S3 Upload → EDAL Descriptor → SQS
"""
import sys
import json
import time
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from .utils.logger import setup_logging, get_logger, print_banner
from .utils.config import get_settings
from .utils.perf_tracker import PerformanceTracker
from .llm.query_generator import QueryGenerator
from .signoz.api_client import SigNozClient
from .signoz.fetcher import SigNozFetcher
from .storage.raw_s3_storage import RawS3Storage
from .storage.edal_descriptor import EDALDescriptorGenerator
from .storage.local_storage import LocalStorage
from .queue.sqs_client import SQSClient

setup_logging()
logger = get_logger(__name__)


class IncidentLogAnalyzer:
    """
    Main orchestrator for incident log analysis with RAW data pipeline.
    
    Workflow:
    1. Receive incident payload from SQS
    2. Generate SigNoz query via LLM with dry-run validation
    3. Automatic fallback to ALL mode if filter returns no results
    4. Fetch RAW logs, metrics, and traces (multi-signal)
    5. Upload RAW data to S3 with partitioned structure
    6. Generate EDAL datasource descriptors
    7. Send completion message to SQS output queue
    8. Continue listening indefinitely
    """
    
    def __init__(self):
        """Initialize the analyzer with all components."""
        self.settings = get_settings()
        
        # Initialize components that don't require SigNoz credentials
        self.query_generator = QueryGenerator()
        self.raw_s3_storage = RawS3Storage()
        self.edal_generator = EDALDescriptorGenerator()
        self.local_storage = LocalStorage()
        
        # SigNoz client and fetcher will be initialized per-message with credentials from payload
        self.signoz_client = None
        self.signoz_fetcher = None
        
        # Initialize SQS client if enabled
        self.sqs_client = None
        if self.settings.sqs_enabled and self.settings.sqs_input_queue_url:
            # Use sqs_endpoint_url if provided, otherwise fall back to localstack_endpoint for backward compatibility
            sqs_endpoint = self.settings.sqs_endpoint_url or (
                self.settings.localstack_endpoint if self.settings.use_localstack else None
            )
            self.sqs_client = SQSClient(
                input_queue_url=self.settings.sqs_input_queue_url,
                output_queue_url=self.settings.sqs_output_queue_url,
                region=self.settings.aws_region,
                endpoint_url=sqs_endpoint
            )
        
        # Print startup banner with configuration
        banner_items = {
            "mode": "RAW Multi-Signal Pipeline",
            "environment": "LocalStack (Dev)" if self.settings.is_local_environment else "AWS (Production)",
            "s3_bucket": self.settings.s3_bucket_name,
            "signoz_credentials": "Loaded from payload",
        }
        
        if self.settings.sqs_enabled:
            banner_items["sqs_enabled"] = "Yes"
            banner_items["input_queue"] = self.settings.sqs_input_queue_url or "Not configured"
            banner_items["output_queue"] = self.settings.sqs_output_queue_url or "Not configured"
        else:
            banner_items["sqs_enabled"] = "No (Using test payload)"
        
        print_banner("🚀 API LOGS FETCHING AGENT STARTED", banner_items)
        
        logger.info(
            "api_fetcher_started_successfully",
            storage_backend="localstack" if self.settings.is_local_environment else "aws_s3",
            s3_bucket=self.settings.s3_bucket_name,
            sqs_enabled=self.settings.sqs_enabled
        )
    
    def process_incident(
        self,
        incident_payload: Dict[str, Any],
        signoz_api_endpoint: str,
        signoz_api_key: str,
        initial_lookback_hours: int = 1,
        tenant: str = "default",
        environment: str = "prod",
        generate_edal: bool = True
    ) -> Dict[str, Any]:
        """
        Process an incident end-to-end with RAW multi-signal pipeline.
        
        Args:
            incident_payload: Incident data from alerting system
            signoz_api_endpoint: SigNoz API endpoint URL from payload
            signoz_api_key: SigNoz API key from payload
            initial_lookback_hours: Historical lookback window (default: 1 hour)
            tenant: Tenant identifier (default: "default")
            environment: Environment (prod, stage, dev)
            generate_edal: Whether to generate EDAL descriptors (default: True)
            
        Returns:
            Processing summary with status, metrics, and S3 locations
        """
        incident_id = incident_payload.get("incident_id", "unknown")
        service_name = incident_payload.get("service", {}).get("name", "unknown")
        
        # Initialize SigNoz clients with credentials from payload
        signoz_client = SigNozClient(api_endpoint=signoz_api_endpoint, api_key=signoz_api_key)
        signoz_fetcher = SigNozFetcher(api_endpoint=signoz_api_endpoint, api_key=signoz_api_key)
        
        # Initialize performance tracker
        perf_tracker = PerformanceTracker(
            incident_id=incident_id,
            output_dir="performance_reports"
        )
        
        # Print payload received banner
        print_banner("📨 RECEIVED NEW PAYLOAD", {
            "incident_id": incident_id,
            "title": incident_payload.get("title", "N/A"),
            "service": service_name,
            "tenant": tenant,
            "environment": environment,
            "lookback_hours": f"{initial_lookback_hours} hour(s)"
        })
        
        logger.info(
            "received_payload",
            incident_id=incident_id,
            service=service_name
        )
        
        start_time = datetime.now(timezone.utc)
        uploaded_files = []
        
        try:
            # Step 1: Generate separate filters for logs, metrics, and traces
            logger.info("generating_llm_query", incident_id=incident_id)
            
            perf_metrics = perf_tracker.start("llm", "query_generation")
            
            query_result = self.query_generator.generate_signoz_query(
                incident_payload=incident_payload,
                lookback_hours=initial_lookback_hours,
                signoz_client=signoz_client,
                enable_dry_run=True
            )
            
            perf_tracker.finish(
                perf_metrics,
                fetch_mode=query_result['metadata'].get('fetch_mode'),
                relaxation_attempts=len(query_result['metadata'].get('relaxation_history', []))
            )
            
            # Extract separate filters for each signal type
            logs_filter = query_result['filters']['logs']
            traces_filter = query_result['filters']['traces']
            metrics_config = query_result['filters']['metrics']
            fetch_mode = query_result['metadata'].get('fetch_mode', 'FILTERED')
            
            # Print brief filter summary
            filter_summary = {
                "fetch_mode": fetch_mode,
                "logs_filter": logs_filter[:80] + "..." if logs_filter and len(logs_filter) > 80 else logs_filter or "None",
                "traces_filter": traces_filter[:80] + "..." if traces_filter and len(traces_filter) > 80 else traces_filter or "None",
                "metrics": metrics_config.get('metric_name', 'signoz_calls_total') if metrics_config else 'signoz_calls_total'
            }
            
            print_banner("✅ LLM QUERY GENERATED", filter_summary)
            
            logger.info(
                "llm_query_generated_successfully",
                incident_id=incident_id,
                fetch_mode=fetch_mode
            )
            
            # Step 2: Calculate time window
            end_time_dt = datetime.now(timezone.utc)
            start_time_dt = end_time_dt - timedelta(hours=initial_lookback_hours)
            start_ms = int(start_time_dt.timestamp() * 1000)
            end_ms = int(end_time_dt.timestamp() * 1000)
            
            # Step 3: Fetch RAW multi-signal data using concurrent fetching with pagination
            logger.info("fetching_data_from_signoz", incident_id=incident_id)
            
            # Extract metric config
            metric_name = "signoz_calls_total"
            aggregation = "rate"
            group_by = []
            
            if metrics_config:
                metric_name = metrics_config.get("metric_name", metric_name)
                aggregation = metrics_config.get("aggregation", aggregation)
                group_by = metrics_config.get("group_by", [])
            
            # Fetch all signals concurrently with pagination (no limits)
            perf_metrics = perf_tracker.start("signoz", "fetch_all_signals_concurrent")
            
            signals_data = signoz_fetcher.fetch_all_signals_concurrent(
                start_ms=start_ms,
                end_ms=end_ms,
                logs_filter=logs_filter if fetch_mode != "ALL_SIGNALS" else "",
                traces_filter=traces_filter if fetch_mode != "ALL_SIGNALS" else "",
                metric_name=metric_name,
                metric_aggregation=aggregation,
                metric_group_by=group_by,
                use_pagination=True,  # Enable pagination to fetch ALL data
                max_pages=None,  # No limit on pages (fetch all available data)
                incident_id=incident_id
            )
            
            # Count fetched data
            log_count = self._count_signal_rows(signals_data.get("logs", {}))
            trace_count = self._count_signal_rows(signals_data.get("traces", {}))
            metric_count = self._count_metric_series(signals_data.get("metrics", {}))
            
            perf_tracker.finish(
                perf_metrics,
                logs_fetched=log_count,
                traces_fetched=trace_count,
                metrics_fetched=metric_count
            )
            
            # Print fetch results with performance
            fetch_duration_ms = perf_metrics.get('duration_ms', 0) if isinstance(perf_metrics, dict) else 0
            fetch_summary = {
                "logs_fetched": f"{log_count} rows",
                "traces_fetched": f"{trace_count} rows",
                "metrics_fetched": f"{metric_count} series",
                "fetch_duration": f"{fetch_duration_ms:.0f} ms" if fetch_duration_ms else "N/A"
            }
            
            print_banner("✅ DATA FETCHED FROM SIGNOZ", fetch_summary)
            
            logger.info(
                "data_fetched_successfully",
                incident_id=incident_id,
                logs=log_count,
                traces=trace_count,
                metrics=metric_count
            )
            
            # Fallback logic for traces if no data with filter
            trace_count = self._count_signal_rows(signals_data.get("traces", {}))
            if trace_count == 0 and traces_filter and fetch_mode != "ALL_SIGNALS" and "error" not in signals_data.get("traces", {}):
                logger.warning(
                    "no_traces_with_filter_retrying_without_filter",
                    incident_id=incident_id,
                    original_filter=traces_filter
                )
                
                # Retry traces without filter using pagination
                try:
                    signals_data["traces"] = signoz_fetcher.fetch_traces_paginated(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        filter_expression="",  # Empty filter = fetch ALL
                        max_pages=None,
                        incident_id=incident_id
                    )
                    
                    trace_count_all = self._count_signal_rows(signals_data["traces"])
                    logger.info(
                        "traces_fetched_without_filter",
                        incident_id=incident_id,
                        count=trace_count_all
                    )
                except Exception as e:
                    logger.error("failed_to_fetch_traces_without_filter", incident_id=incident_id, error=str(e))
                    signals_data["traces"] = {"error": str(e)}
            
            # Step 4: Upload RAW data to S3 with partitioned structure
            logger.info("uploading_data_to_s3", incident_id=incident_id)
            
            sequence = 1
            for signal, data in signals_data.items():
                # Check if data is valid (not an error response)
                if isinstance(data, dict) and "error" in data and len(data) == 1:
                    logger.warning(
                        f"skipping_{signal}_upload",
                        incident_id=incident_id,
                        error=data.get("error")
                    )
                    continue
                
                try:
                    perf_metrics = perf_tracker.start("s3", f"upload_{signal}")
                    
                    s3_key = self.raw_s3_storage.upload_raw_signal(
                        data=data,
                        signal=signal,
                incident_id=incident_id,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        sequence=sequence,
                        part=1,
                        tenant=tenant,
                        service=service_name,
                        environment=environment,
                        metadata={
                            "fetch_mode": fetch_mode,
                            "logs_filter": logs_filter,
                            "traces_filter": traces_filter,
                            "metrics_config": metrics_config,
                            "original_filters": query_result['metadata'].get('original_filters', {})
                        }
                    )
                    
                    perf_tracker.finish(perf_metrics, s3_key=s3_key)
                    
                    uploaded_files.append({"signal": signal, "s3_key": s3_key})
                    logger.info(f"{signal}_uploaded", incident_id=incident_id)
                except Exception as e:
                    perf_tracker.finish(perf_metrics, success=False, error=str(e))
                    logger.error(f"failed_to_upload_{signal}", incident_id=incident_id, error=str(e))
            
            # Print upload summary
            upload_summary = {
                "bucket": self.settings.s3_bucket_name,
                "files_uploaded": len(uploaded_files),
                "signals": ", ".join([f["signal"] for f in uploaded_files])
            }
            print_banner("📤 DATA UPLOADED TO S3", upload_summary)
            
            # Step 5: Upload manifest
            manifest_data = {
                "incident_id": incident_id,
                "service": service_name,
                "tenant": tenant,
                "environment": environment,
                "time_range": {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "lookback_hours": initial_lookback_hours
                },
                "query_info": query_result['metadata'],
                "uploaded_files": uploaded_files,
                "upload_count": len(uploaded_files),
                "processing_time_seconds": int((datetime.now(timezone.utc) - start_time).total_seconds())
            }
            
            # Only upload manifests for successfully uploaded signals
            uploaded_signals = [f["signal"] for f in uploaded_files]
            for signal in uploaded_signals:
                try:
                    manifest_key = self.raw_s3_storage.upload_manifest(
                        incident_id=incident_id,
                        signal=signal,
                        manifest_data=manifest_data,
                        tenant=tenant,
                        service=service_name,
                        environment=environment
                    )
                    logger.info(f"{signal}_manifest_uploaded", incident_id=incident_id, manifest_key=manifest_key)
                except Exception as e:
                    logger.error(f"failed_to_upload_{signal}_manifest", incident_id=incident_id, error=str(e))
            
            # Step 6: Generate EDAL datasource descriptors
            edal_descriptor = None
            edal_s3_key = None
            
            if generate_edal and uploaded_files:
                try:
                    logger.info("step_4_generating_edal_descriptor", incident_id=incident_id)
                    
                    perf_metrics = perf_tracker.start("edal", "generate_descriptor")
                    
                    company_id = tenant
                    # Only include successfully uploaded signals in EDAL descriptor
                    signals_list = uploaded_signals
                    
                    edal_descriptor, edal_s3_key = self.edal_generator.generate_and_save(
                        company_id=company_id,
                        tenant=tenant,
                        service=service_name,
                        s3_client=self.raw_s3_storage.s3_client,
                        environment=environment,
                        signals=signals_list,
                        use_iam_role=False,  # Use access key placeholders for now
                        role_arn=None
                    )
                    
                    perf_tracker.finish(perf_metrics, edal_s3_key=edal_s3_key, signals_count=len(signals_list))
                    
                    logger.info(
                        "edal_descriptor_generated",
                        incident_id=incident_id,
                        edal_s3_key=edal_s3_key,
                        company_id=company_id
                    )
                except Exception as e:
                    perf_tracker.finish(perf_metrics, success=False, error=str(e))
                    logger.error("failed_to_generate_edal_descriptor", incident_id=incident_id, error=str(e))
            
            # Step 7: Save local copy for reference
            try:
                local_summary = {
                    "incident_id": incident_id,
                    "signals_fetched": list(signals_data.keys()),
                    "uploaded_files": uploaded_files,
                    "manifest": manifest_data,
                    "edal_descriptor_key": edal_s3_key
                }
                self.local_storage.save_logs(
                    logs=[local_summary],
                    incident_id=incident_id,
                    metadata={"type": "processing_summary"}
                )
            except Exception as e:
                logger.warning("failed_to_save_local_summary", incident_id=incident_id, error=str(e))
            
            # Create final summary
            end_time = datetime.now(timezone.utc)
            summary = {
                "status": "completed",
                "incident_id": incident_id,
                "completed_at": end_time.isoformat(),
                "processing_time_seconds": int((end_time - start_time).total_seconds()),
                "query_info": {
                    "logs_filter": logs_filter,
                    "traces_filter": traces_filter,
                    "metrics_config": metrics_config,
                    "original_filters": query_result['metadata'].get('original_filters', {}),
                    "fetch_mode": fetch_mode,
                    "relaxation_history": query_result['metadata'].get('relaxation_history', []),
                    "llm_model": "anthropic.claude-3-5-sonnet-20241022-v2:0"
                },
                "signals": {
                    "fetched": list(signals_data.keys()),
                    "count": len(signals_data)
                },
                "storage": {
                    "uploaded_files": uploaded_files,
                    "upload_count": len(uploaded_files),
                    "edal_descriptor_key": edal_s3_key,
                    "s3_bucket": self.settings.s3_bucket_name,
                    "storage_structure": f"raw/{environment}/{tenant}/{service_name}/{{signal}}/date=YYYY-MM-DD/hour=HH/"
                }
            }
            
            logger.info(
                "processing_incident_completed",
                incident_id=incident_id,
                signals_fetched=len(signals_data),
                files_uploaded=len(uploaded_files),
                processing_time=summary["processing_time_seconds"]
            )
            
            # Save and print performance report
            try:
                perf_report_path = perf_tracker.save_report()
                perf_tracker.print_summary()
                summary["performance_report"] = perf_report_path
                
                logger.info(
                    "performance_report_generated",
                    incident_id=incident_id,
                    report_path=perf_report_path
                )
            except Exception as e:
                logger.warning("failed_to_save_performance_report", incident_id=incident_id, error=str(e))
            
            return summary
            
        except KeyboardInterrupt:
            logger.warning(
                "processing_incident_interrupted",
                incident_id=incident_id
            )
            return {
                "status": "interrupted",
                "incident_id": incident_id,
                "message": "Processing interrupted by user",
                "uploaded_files": uploaded_files
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
                "error_type": type(e).__name__,
                "uploaded_files": uploaded_files
            }
    
    def _count_signal_rows(self, signoz_response: Dict[str, Any]) -> int:
        """Count rows in SigNoz API response.
        
        Args:
            signoz_response: Raw SigNoz API response
            
        Returns:
            Number of rows in the response
        """
        try:
            results = signoz_response.get("data", {}).get("results", [])
            if results and len(results) > 0:
                rows = results[0].get("rows")
                return len(rows) if rows else 0
            return 0
        except Exception as e:
            logger.warning("failed_to_count_rows", error=str(e))
            return 0
    
    def _count_metric_series(self, signoz_response: Dict[str, Any]) -> int:
        """Count metric series in SigNoz API response.
        
        Args:
            signoz_response: Raw SigNoz API response for metrics
            
        Returns:
            Number of series in the response
        """
        try:
            results = signoz_response.get("data", {}).get("results", [])
            if results and len(results) > 0:
                series = results[0].get("series")
                return len(series) if series else 0
            return 0
        except Exception as e:
            logger.warning("failed_to_count_metric_series", error=str(e))
            return 0
    
    def handle_sqs_message(self, payload: Dict[str, Any]) -> bool:
        """Handle a single SQS message.
        
        Args:
            payload: Raw payload from SQS (will be validated)
            
        Returns:
            True if processing successful, False otherwise
        """
        start_time = time.time()
        
        try:
            # Import validation function
            from .utils.sqs_schema import validate_and_extract_payload
            
            # Validate payload and extract SigNoz credentials
            incident, signoz_api_endpoint, signoz_api_key = validate_and_extract_payload(payload)
            
            # Extract parameters from incident section
            incident_id = incident.get("incident_id", "unknown")
            company_id = incident.get("company_id", "default")
            lookback_hours = incident.get("lookback_hours", 1)
            environment = incident.get("environment", "prod")
            service_name = incident.get("service", {}).get("name", "unknown")
            
            # Use company_id as tenant
            tenant = company_id
            
            # Print incoming payload (sanitized - hide API key)
            print("\n" + "="*80)
            print("📥 INCOMING INCIDENT PAYLOAD")
            print("="*80)
            sanitized_payload = json.loads(json.dumps(payload))
            if "data_sources" in sanitized_payload:
                for ds in sanitized_payload["data_sources"]:
                    if "auth_config" in ds and "api_key" in ds["auth_config"]:
                        ds["auth_config"]["api_key"] = "***REDACTED***"
            print(json.dumps(sanitized_payload, indent=2))
            print("="*80)
            print(f"✅ Payload validated successfully")
            print(f"   Incident ID: {incident_id}")
            print(f"   Company ID: {company_id}")
            print(f"   Service: {service_name}")
            print(f"   Environment: {environment}")
            print(f"   Lookback: {lookback_hours} hour(s)")
            print(f"   SigNoz Endpoint: {signoz_api_endpoint}")
            print("="*80 + "\n")
            
            logger.info(
                "processing_sqs_message",
                incident_id=incident_id,
                company_id=company_id,
                tenant=tenant,
                environment=environment,
                signoz_endpoint=signoz_api_endpoint
            )
            
            print(f"🤖 Generating LLM filter query for incident {incident_id}...")
            
            # Process incident with credentials from payload
            result = self.process_incident(
                incident_payload=incident,
                signoz_api_endpoint=signoz_api_endpoint,
                signoz_api_key=signoz_api_key,
                initial_lookback_hours=lookback_hours,
                tenant=tenant,
                environment=environment,
                generate_edal=True
            )
            
            # Prepare S3 URLs for completion message
            s3_urls = {}
            for file_info in result.get('storage', {}).get('uploaded_files', []):
                signal = file_info.get('signal')
                s3_key = file_info.get('s3_key', '')
                if signal and s3_key:
                    # Convert S3 key to full URL
                    s3_urls[signal] = self.raw_s3_storage.get_s3_url_from_key(s3_key)
            
            # Print S3 storage details
            print("\n" + "="*80)
            print("💾 S3 STORAGE DETAILS")
            print("="*80)
            print(f"Bucket: {self.settings.s3_bucket_name}")
            print(f"Signals stored: {', '.join(s3_urls.keys())}")
            for signal, url in s3_urls.items():
                print(f"   {signal}: {url}")
            if result.get('storage', {}).get('edal_descriptor_key'):
                print(f"EDAL Descriptor: {result.get('storage', {}).get('edal_descriptor_key')}")
            print("="*80 + "\n")
            
            # Send completion message to output queue in new format
            if self.sqs_client:
                self.sqs_client.send_completion_message(
                    incident_id=incident_id,
                    company_id=company_id,
                    service=service_name,
                    environment=environment,
                    s3_urls=s3_urls
                )
                
                # Print completion message
                print("\n" + "="*80)
                print("✅ JOB COMPLETED - OUTPUT QUEUE MESSAGE")
                print("="*80)
                completion_payload = {
                    "incident": {
                        "incident_id": incident_id,
                        "company_id": company_id,
                        "service": service_name,
                        "env": environment
                    },
                    "sources": {
                        "signoz": s3_urls
                    }
                }
                print(json.dumps(completion_payload, indent=2))
                print("="*80)
                print(f"📤 Completion message sent to output queue")
                print(f"   Queue: {self.settings.sqs_output_queue_url}")
                print("="*80 + "\n")
            
            return result['status'] == 'completed'
            
        except Exception as e:
            logger.error(
                "sqs_message_handling_failed",
                incident_id=incident_id if 'incident_id' in locals() else "unknown",
                error=str(e)
            )
            
            # Note: For failures, we don't send output messages in the new format
            # The absence of a message indicates failure
            
            return False
    
    def start_sqs_polling(self):
        """Start SQS polling loop."""
        if not self.sqs_client:
            logger.error("sqs_client_not_initialized")
            return
        
        # Print listening banner
        print_banner("👂 LISTENING FOR NEW PAYLOADS", {
            "input_queue": self.settings.sqs_input_queue_url,
            "poll_interval": f"{self.settings.sqs_poll_interval}s",
            "tip": "Run 'python send_test_message.py' to send a test payload"
        })
        
        logger.info("listening_for_sqs_messages")
        
        self.sqs_client.start_polling(
            message_handler=self.handle_sqs_message,
            poll_interval=self.settings.sqs_poll_interval,
            max_empty_polls=self.settings.sqs_max_empty_polls
        )


def main():
    """
    Main entry point for production deployment - SQS Mode Only.
    
    The service runs indefinitely, listening to the SQS input queue for incident payloads.
    Configuration is provided via environment variables.
    
    Usage:
        python -m src.main
    """
    # Initialize analyzer
    analyzer = IncidentLogAnalyzer()
    
    # Check if SQS is enabled
    if not analyzer.settings.sqs_enabled or not analyzer.settings.sqs_input_queue_url:
        logger.error("sqs_not_configured")
        print("\n❌ ERROR: SQS is not configured!")
        print("Please set the following environment variables:")
        print("  - SQS_ENABLED=true")
        print("  - SQS_INPUT_QUEUE_URL=<your-input-queue-url>")
        print("  - SQS_OUTPUT_QUEUE_URL=<your-output-queue-url>")
        sys.exit(1)
    
    # Start SQS polling (runs indefinitely)
    logger.info("starting_sqs_polling_mode")
    
    try:
        analyzer.start_sqs_polling()
    except KeyboardInterrupt:
        logger.info("polling_interrupted_by_user")
        print("\n\n⚠️  Service stopped by user\n")
        sys.exit(130)
    except Exception as e:
        logger.error("unexpected_error_in_main_loop", error=str(e), exc_info=True)
        print(f"\n\n❌ Unexpected error: {str(e)}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
