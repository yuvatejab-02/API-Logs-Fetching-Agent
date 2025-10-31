#!/usr/bin/env python3
"""
Main Production Script - Incident Log Analyzer
Handles: SQS → Payload → LLM Query → Multi-Signal Fetch → Raw S3 Upload → EDAL Descriptor → SQS
"""
import sys
import json
import os
import time
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from .utils.logger import setup_logging, get_logger
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
    
    NEW Workflow:
    1. Receive incident payload
    2. Generate SigNoz query via LLM with dry-run validation
    3. Automatic fallback to ALL mode if filter returns no results
    4. Fetch RAW logs, metrics, and traces (multi-signal)
    5. Upload RAW data to S3 with partitioned structure
    6. Generate EDAL datasource descriptors
    7. No transformation - preserve original SigNoz response
    """
    
    def __init__(self):
        """Initialize the analyzer with all components."""
        self.settings = get_settings()
        
        # Initialize components
        self.query_generator = QueryGenerator()
        self.signoz_client = SigNozClient()
        self.signoz_fetcher = SigNozFetcher(
            api_endpoint=self.settings.signoz_api_endpoint,
            api_key=self.settings.signoz_api_key
        )
        self.raw_s3_storage = RawS3Storage()
        self.edal_generator = EDALDescriptorGenerator()
        self.local_storage = LocalStorage()
        
        # Initialize SQS client if enabled
        self.sqs_client = None
        if self.settings.sqs_enabled and self.settings.sqs_input_queue_url:
            self.sqs_client = SQSClient(
                input_queue_url=self.settings.sqs_input_queue_url,
                output_queue_url=self.settings.sqs_output_queue_url,
                region=self.settings.aws_region,
                endpoint_url=self.settings.localstack_endpoint if self.settings.use_localstack else None
            )
        
        logger.info(
            "incident_log_analyzer_initialized",
            storage_backend="localstack" if self.settings.is_local_environment else "aws_s3",
            s3_bucket=self.settings.s3_bucket_name,
            sqs_enabled=self.settings.sqs_enabled,
            mode="raw_multi_signal"
        )
    
    def process_incident(
        self,
        incident_payload: Dict[str, Any],
        initial_lookback_hours: int = 1,
        tenant: str = "default",
        environment: str = "prod",
        generate_edal: bool = True
    ) -> Dict[str, Any]:
        """
        Process an incident end-to-end with RAW multi-signal pipeline.
        
        NEW Pipeline:
        1. Generate SigNoz query with LLM (includes dry-run validation)
        2. Automatic fallback to ALL mode if no results
        3. Fetch RAW logs and traces (no transformation)
        4. Upload RAW data to S3 with partitioned keys
        5. Generate EDAL datasource descriptors
        
        Args:
            incident_payload: Incident data from alerting system
            initial_lookback_hours: Historical lookback window (default: 1 hour)
            tenant: Tenant identifier (default: "default")
            environment: Environment (prod, stage, dev)
            generate_edal: Whether to generate EDAL descriptors (default: True)
            
        Returns:
            Processing summary with status, metrics, and S3 locations
        """
        incident_id = incident_payload.get("incident_id", "unknown")
        service_name = incident_payload.get("service", {}).get("name", "unknown")
        
        # Initialize performance tracker
        perf_tracker = PerformanceTracker(
            incident_id=incident_id,
            output_dir="performance_reports"
        )
        
        logger.info(
            "processing_incident_started",
            incident_id=incident_id,
            title=incident_payload.get("title"),
            service=service_name,
            tenant=tenant,
            environment=environment,
            lookback_hours=initial_lookback_hours
        )
        
        start_time = datetime.now(timezone.utc)
        uploaded_files = []
        
        try:
            # Step 1: Generate separate filters for logs, metrics, and traces
            logger.info("step_1_generating_filters_with_validation", incident_id=incident_id)
            
            perf_metrics = perf_tracker.start("llm", "query_generation")
            
            query_result = self.query_generator.generate_signoz_query(
                incident_payload=incident_payload,
            lookback_hours=initial_lookback_hours,
                signoz_client=self.signoz_client,
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
            
            logger.info(
                "filters_generated",
                incident_id=incident_id,
                logs_filter=logs_filter,
                traces_filter=traces_filter,
                has_metrics=metrics_config is not None,
                fetch_mode=fetch_mode,
                relaxation_history=query_result['metadata'].get('relaxation_history', [])
            )
            
            # Step 2: Calculate time window
            end_time_dt = datetime.now(timezone.utc)
            start_time_dt = end_time_dt - timedelta(hours=initial_lookback_hours)
            start_ms = int(start_time_dt.timestamp() * 1000)
            end_ms = int(end_time_dt.timestamp() * 1000)
            
            # Step 3: Fetch RAW multi-signal data using concurrent fetching with pagination
            logger.info("step_2_fetching_raw_multi_signal_data_concurrent", incident_id=incident_id)
            
            # Print LLM-generated filters
            print(f"✅ LLM query generated successfully")
            print(f"   Fetch Mode: {fetch_mode}")
            if logs_filter:
                print(f"   Logs Filter: {logs_filter[:100]}...")
            if traces_filter:
                print(f"   Traces Filter: {traces_filter[:100]}...")
            print(f"   Metrics: {metrics_config.get('metric_name', 'signoz_calls_total') if metrics_config else 'signoz_calls_total'}")
            print()
            
            # Extract metric config
            metric_name = "signoz_calls_total"
            aggregation = "rate"
            group_by = []
            
            if metrics_config:
                metric_name = metrics_config.get("metric_name", metric_name)
                aggregation = metrics_config.get("aggregation", aggregation)
                group_by = metrics_config.get("group_by", [])
            
            print(f"🔄 Fetching data from SigNoz (concurrent + pagination)...")
            print(f"   Time range: {initial_lookback_hours} hour(s) lookback")
            print(f"   Signals: logs, traces, metrics")
            print()
            
            # Fetch all signals concurrently with pagination (no limits)
            perf_metrics = perf_tracker.start("signoz", "fetch_all_signals_concurrent")
            
            signals_data = self.signoz_fetcher.fetch_all_signals_concurrent(
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
            
            perf_tracker.finish(
                perf_metrics,
                logs_fetched=self._count_signal_rows(signals_data.get("logs", {})),
                traces_fetched=self._count_signal_rows(signals_data.get("traces", {})),
                metrics_fetched=self._count_metric_series(signals_data.get("metrics", {}))
            )
            
            # Print fetch results
            log_count = self._count_signal_rows(signals_data.get("logs", {}))
            trace_count = self._count_signal_rows(signals_data.get("traces", {}))
            metric_count = self._count_metric_series(signals_data.get("metrics", {}))
            
            print(f"✅ Data fetched successfully")
            print(f"   Logs: {log_count} rows")
            print(f"   Traces: {trace_count} rows")
            print(f"   Metrics: {metric_count} series")
            print()
            
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
                    signals_data["traces"] = self.signoz_fetcher.fetch_traces_paginated(
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
            logger.info("step_3_uploading_raw_data_to_s3", incident_id=incident_id)
            
            print(f"📤 Uploading data to S3...")
            print(f"   Bucket: {self.settings.s3_bucket_name}")
            print()
            
            sequence = 1
            for signal, data in signals_data.items():
                # Check if data is valid (not an error response)
                if isinstance(data, dict) and "error" in data and len(data) == 1:
                    logger.warning(
                        f"skipping_{signal}_upload_due_to_error",
                        incident_id=incident_id,
                        error=data.get("error")
                    )
                    print(f"   ⚠️  Skipping {signal} (error: {data.get('error')})")
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
                    logger.info(f"{signal}_uploaded_to_s3", incident_id=incident_id, s3_key=s3_key)
                    print(f"   ✅ {signal.capitalize()} uploaded: {s3_key.split('/')[-1]}")
                except Exception as e:
                    perf_tracker.finish(perf_metrics, success=False, error=str(e))
                    logger.error(f"failed_to_upload_{signal}", incident_id=incident_id, error=str(e), exc_info=True)
                    print(f"   ❌ Failed to upload {signal}: {str(e)}")
            
            print()
            
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
            payload: Validated incident payload from SQS
            
        Returns:
            True if processing successful, False otherwise
        """
        incident_id = payload.get("incident_id", "unknown")
        start_time = time.time()
        
        try:
            # Extract parameters from payload
            lookback_hours = payload.get("lookback_hours", 1)
            tenant = payload.get("tenant", "default")
            environment = payload.get("environment", "prod")
            service_name = payload.get("service", {}).get("name", "unknown")
            
            # Print incoming payload
            print("\n" + "="*80)
            print("📥 INCOMING INCIDENT PAYLOAD")
            print("="*80)
            print(json.dumps(payload, indent=2))
            print("="*80)
            print(f"✅ Payload validated successfully")
            print(f"   Incident ID: {incident_id}")
            print(f"   Service: {service_name}")
            print(f"   Environment: {environment}")
            print(f"   Lookback: {lookback_hours} hour(s)")
            print("="*80 + "\n")
            
            logger.info(
                "processing_sqs_message",
                incident_id=incident_id,
                tenant=tenant,
                environment=environment
            )
            
            print(f"🤖 Generating LLM filter query for incident {incident_id}...")
            
            # Process incident
            result = self.process_incident(
                incident_payload=payload,
                initial_lookback_hours=lookback_hours,
                tenant=tenant,
                environment=environment,
                generate_edal=True
            )
            
            # Prepare S3 keys for completion message
            s3_keys = {}
            for file_info in result.get('storage', {}).get('uploaded_files', []):
                signal = file_info.get('signal')
                if signal:
                    s3_keys[signal] = file_info.get('s3_key', '')
            
            # Print S3 storage details
            print("\n" + "="*80)
            print("💾 S3 STORAGE DETAILS")
            print("="*80)
            print(f"Bucket: {self.settings.s3_bucket_name}")
            print(f"Signals stored: {', '.join(s3_keys.keys())}")
            for signal, key in s3_keys.items():
                print(f"   {signal}: {key}")
            if result.get('storage', {}).get('edal_descriptor_key'):
                print(f"EDAL Descriptor: {result.get('storage', {}).get('edal_descriptor_key')}")
            print("="*80 + "\n")
            
            # Send completion message to output queue
            completion_payload = None
            if self.sqs_client:
                completion_payload = {
                    "incident_id": incident_id,
                    "status": result['status'],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "processing_time_seconds": round(time.time() - start_time, 2),
                    "signals_fetched": result.get('signals', {}).get('fetched', []),
                    "s3_keys": s3_keys,
                    "edal_descriptor_key": result.get('storage', {}).get('edal_descriptor_key'),
                    "error_message": result.get('error') if result['status'] != 'completed' else None
                }
                
                self.sqs_client.send_completion_message(
                    incident_id=incident_id,
                    status=result['status'],
                    timestamp=completion_payload['timestamp'],
                    processing_time=completion_payload['processing_time_seconds'],
                    signals_fetched=completion_payload['signals_fetched'],
                    s3_keys=s3_keys,
                    edal_descriptor_key=result.get('storage', {}).get('edal_descriptor_key'),
                    error_message=completion_payload.get('error_message')
                )
                
                # Print completion message
                print("\n" + "="*80)
                print("✅ JOB COMPLETED - OUTPUT QUEUE MESSAGE")
                print("="*80)
                print(json.dumps(completion_payload, indent=2))
                print("="*80)
                print(f"📤 Completion message sent to output queue")
                print(f"   Queue: {self.settings.sqs_output_queue_url}")
                print("="*80 + "\n")
            
            return result['status'] == 'completed'
            
        except Exception as e:
            logger.error(
                "sqs_message_handling_failed",
                incident_id=incident_id,
                error=str(e)
            )
            
            # Send failure message to output queue
            if self.sqs_client:
                try:
                    self.sqs_client.send_completion_message(
                        incident_id=incident_id,
                        status="failed",
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        processing_time=time.time() - start_time,
                        signals_fetched=[],
                        s3_keys={},
                        error_message=str(e)
                    )
                except Exception as send_error:
                    logger.error("failed_to_send_error_completion", error=str(send_error))
            
            return False
    
    def start_sqs_polling(self):
        """Start SQS polling loop."""
        if not self.sqs_client:
            logger.error("sqs_client_not_initialized")
            return
        
        # Print startup banner
        print("\n" + "="*80)
        print("🚀 INCIDENT LOG ANALYZER - AUTOMATED PIPELINE")
        print("="*80)
        print(f"Mode: SQS Polling (Continuous)")
        print(f"Input Queue: {self.settings.sqs_input_queue_url}")
        print(f"Output Queue: {self.settings.sqs_output_queue_url}")
        print(f"S3 Bucket: {self.settings.s3_bucket_name}")
        print(f"Poll Interval: {self.settings.sqs_poll_interval}s")
        print("="*80)
        print()
        print("✅ System ready and listening for incident payloads...")
        print("📥 Waiting for messages from input queue...")
        print()
        print("💡 To send a test payload, run:")
        print("   python send_test_message.py --incident-id YOUR_ID")
        print()
        print("="*80 + "\n")
        
        logger.info("starting_sqs_polling")
        
        self.sqs_client.start_polling(
            message_handler=self.handle_sqs_message,
            poll_interval=self.settings.sqs_poll_interval,
            max_empty_polls=self.settings.sqs_max_empty_polls
        )


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
        "--mode",
        type=str,
        choices=["sqs", "file", "json"],
        default="file",
        help="Execution mode: sqs (poll from queue), file (read from file), json (parse JSON string)"
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
        "--tenant",
        type=str,
        default="default",
        help="Tenant identifier for S3 partitioning (default: default)"
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="prod",
        choices=["prod", "stage", "dev"],
        help="Environment (prod, stage, dev) for S3 partitioning (default: prod)"
    )
    parser.add_argument(
        "--no-edal",
        action="store_true",
        help="Disable EDAL datasource descriptor generation"
    )
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = IncidentLogAnalyzer()
    
    # SQS Mode - Poll from queue
    if args.mode == "sqs":
        logger.info("starting_in_sqs_mode")
        print("\n" + "="*80)
        print("  🚨 INCIDENT LOG ANALYZER - SQS POLLING MODE")
        print("="*80)
        print(f"\nInput Queue: {analyzer.settings.sqs_input_queue_url}")
        print(f"Output Queue: {analyzer.settings.sqs_output_queue_url}")
        print(f"Poll Interval: {analyzer.settings.sqs_poll_interval}s")
        
        if analyzer.settings.sqs_max_empty_polls is None:
            print(f"Polling Mode: CONTINUOUS (will run indefinitely)")
        else:
            print(f"Max Empty Polls: {analyzer.settings.sqs_max_empty_polls}")
        
        print("\n💡 Send messages using: python send_test_message.py")
        print("💡 Check output queue: python check_output_queue.py")
        print("\nWaiting for messages...")
        print("="*80 + "\n")
        
        try:
            analyzer.start_sqs_polling()
        except KeyboardInterrupt:
            print("\n\n⚠️  Polling interrupted by user\n")
            sys.exit(130)
        
        sys.exit(0)
    
    # File/JSON Mode - Get incident payload from various sources
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
    print("  🚨 INCIDENT LOG ANALYZER - RAW MULTI-SIGNAL MODE")
    print("="*80)
    print(f"\nIncident ID: {incident_payload.get('incident_id')}")
    print(f"Title: {incident_payload.get('title', 'N/A')}")
    print(f"Service: {incident_payload.get('service', {}).get('name', 'N/A')}")
    print(f"Tenant: {args.tenant}")
    print(f"Environment: {args.environment}")
    print(f"Lookback: {args.lookback_hours} hour(s)")
    print(f"EDAL Generation: {'Enabled' if not args.no_edal else 'Disabled'}")
    print(f"Mode: RAW (No Transformation)")
    print("\n" + "="*80 + "\n")
    
    try:
        result = analyzer.process_incident(
            incident_payload=incident_payload,
            initial_lookback_hours=args.lookback_hours,
            tenant=args.tenant,
            environment=args.environment,
            generate_edal=not args.no_edal
        )
        
        # Print results
        print("\n" + "="*80)
        if result["status"] == "completed":
            print("  ✅ ANALYSIS COMPLETED SUCCESSFULLY")
            print("="*80)
            print(f"\n📊 Summary:")
            print(f"   Processing Time: {result['processing_time_seconds']}s")
            print(f"   Signals Fetched: {', '.join(result['signals']['fetched'])}")
            print(f"   Files Uploaded: {result['storage']['upload_count']}")
            print(f"\n🔍 Query Info:")
            print(f"   Fetch Mode: {result['query_info']['fetch_mode']}")
            print(f"   Logs Filter: {result['query_info']['logs_filter']}")
            print(f"   Traces Filter: {result['query_info']['traces_filter']}")
            if result['query_info'].get('metrics_config'):
                print(f"   Metrics: {result['query_info']['metrics_config'].get('metric_name', 'N/A')}")
            if result['query_info'].get('relaxation_history'):
                print(f"   Relaxation Attempts: {len(result['query_info']['relaxation_history'])}")
            print(f"\n💾 Storage:")
            print(f"   S3 Bucket: {result['storage']['s3_bucket']}")
            print(f"   Structure: {result['storage']['storage_structure']}")
            if result['storage'].get('edal_descriptor_key'):
                print(f"   EDAL Descriptor: {result['storage']['edal_descriptor_key']}")
            print(f"\n📁 Uploaded Files:")
            for file_info in result['storage']['uploaded_files']:
                print(f"   - {file_info['signal']}: {file_info['s3_key']}")
        else:
            print("  ❌ ANALYSIS FAILED")
            print("="*80)
            print(f"\nError: {result.get('error', 'Unknown error')}")
            if result.get('uploaded_files'):
                print(f"\nPartially uploaded files: {len(result['uploaded_files'])}")
        
        print("\n" + "="*80 + "\n")
        
        sys.exit(0 if result["status"] == "completed" else 1)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user\n")
        sys.exit(130)


if __name__ == "__main__":
    main()
