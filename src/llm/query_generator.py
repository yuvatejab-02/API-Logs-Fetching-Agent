"""LLM-based SigNoz query generator using AWS Bedrock."""
import json
import time
from typing import Dict, Any, Tuple
from datetime import datetime, timedelta, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ..utils.config import get_settings
from ..utils.logger import get_logger
from .prompts import SYSTEM_PROMPT, get_query_generation_prompt
from .filter_relaxation import FilterRelaxation

logger = get_logger(__name__)


class QueryGenerator:
    """Generates SigNoz API queries using Claude via AWS Bedrock."""
    
    def __init__(self):
        """Initialize the query generator with Bedrock client."""
        settings = get_settings()
        
        # Configure boto3 client with extended timeout for Bedrock
        bedrock_config = Config(
            region_name=settings.bedrock_region,
            read_timeout=300,  # 5 minutes
            connect_timeout=60,
            retries={'max_attempts': 3, 'mode': 'standard'}
        )
        
        # Initialize Bedrock Runtime client
        self.client = boto3.client(
            service_name='bedrock-runtime',
            region_name=settings.bedrock_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            config=bedrock_config,
            endpoint_url=settings.bedrock_endpoint_url
        )
        
        self.model_id = settings.bedrock_model_id
        self.anthropic_version = "bedrock-2023-05-31"
        
        logger.info(
            "bedrock_client_initialized",
            model_id=self.model_id,
            region=settings.bedrock_region
        )
        
    def generate_signoz_query(
        self, 
        incident_payload: Dict[str, Any],
        lookback_hours: int = 1,
        signoz_client = None,
        enable_dry_run: bool = True,
        max_relaxation_attempts: int = 2
    ) -> Dict[str, Any]:
        """Generate a SigNoz API query from incident payload with dry-run validation.
        
        Args:
            incident_payload: The incident data
            lookback_hours: How many hours back to search (default: 1)
            signoz_client: Optional SigNozClient for dry-run validation
            enable_dry_run: Whether to enable dry-run validation (default: True)
            max_relaxation_attempts: Maximum filter relaxation attempts (default: 2)
            
        Returns:
            Complete SigNoz API query payload with metadata
            
        Raises:
            Exception: If LLM fails to generate valid query
        """
        incident_id = incident_payload.get("incident_id", "unknown")
        
        logger.info(
            "generating_signoz_query",
            incident_id=incident_id,
            lookback_hours=lookback_hours,
            dry_run_enabled=enable_dry_run
        )
        
        try:
            # Get separate filter expressions from LLM
            llm_result = self._get_filter_from_llm(incident_payload)
            
            logs_filter = llm_result["logs_filter"]
            traces_filter = llm_result["traces_filter"]
            metrics_config = llm_result["metrics_config"]
            reasoning = llm_result["reasoning"]
            key_attrs = llm_result["key_attributes"]
            
            # Calculate time window for dry-run
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=lookback_hours)
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            fetch_mode = "FILTERED"
            relaxation_history = []
            final_logs_filter = logs_filter
            final_traces_filter = traces_filter
            
            # Dry-run validation for logs filter (if enabled)
            if enable_dry_run and signoz_client and logs_filter:
                log_count = signoz_client.dry_run_query(
                    filter_expression=logs_filter,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=10,
                    incident_id=incident_id
                )
                
                logger.info(
                    "dry_run_logs_filter_result",
                    incident_id=incident_id,
                    log_count=log_count,
                    logs_filter=logs_filter
                )
                
                # If no results, try relaxing the logs filter
                if log_count == 0:
                    for attempt in range(max_relaxation_attempts):
                        relaxed_filter, strategy = FilterRelaxation.relax_filter(
                            logs_filter, attempt
                        )
                        
                        relaxation_history.append({
                            "attempt": attempt + 1,
                            "strategy": strategy,
                            "filter": relaxed_filter
                        })
                        
                        if relaxed_filter:
                            log_count = signoz_client.dry_run_query(
                                filter_expression=relaxed_filter,
                                start_ms=start_ms,
                                end_ms=end_ms,
                                limit=10,
                                incident_id=incident_id
                            )
                            
                            logger.info(
                                "dry_run_relaxation_attempt",
                                incident_id=incident_id,
                                attempt=attempt + 1,
                                strategy=strategy,
                                log_count=log_count,
                                relaxed_filter=relaxed_filter
                            )
                            
                            if log_count > 0:
                                final_logs_filter = relaxed_filter
                                fetch_mode = f"RELAXED_{strategy.upper()}"
                                break
                        else:
                            # Empty filter means ALL mode
                            final_logs_filter = ""
                            final_traces_filter = ""
                            fetch_mode = "ALL_SIGNALS"
                            break
                    
                    # If still no results after all attempts, fallback to ALL mode
                    if log_count == 0:
                        logger.warning(
                            "fallback_to_all_signals_mode",
                            incident_id=incident_id,
                            original_logs_filter=logs_filter,
                            relaxation_attempts=len(relaxation_history)
                        )
                        final_logs_filter = ""
                        final_traces_filter = ""
                        fetch_mode = "ALL_SIGNALS"
                else:
                    logger.info(
                        "dry_run_success_using_original_filters",
                        incident_id=incident_id,
                        log_count=log_count
                    )
            
            logger.info(
                "query_generated_successfully",
                incident_id=incident_id,
                logs_filter=final_logs_filter,
                traces_filter=final_traces_filter,
                has_metrics=metrics_config is not None,
                fetch_mode=fetch_mode,
                reasoning=reasoning,
                key_attributes=key_attrs
            )
            
            return {
                "filters": {
                    "logs": final_logs_filter,
                    "traces": final_traces_filter,
                    "metrics": metrics_config
                },
                "metadata": {
                    "original_filters": {
                        "logs": logs_filter,
                        "traces": traces_filter,
                        "metrics": metrics_config
                    },
                    "fetch_mode": fetch_mode,
                    "reasoning": reasoning,
                    "key_attributes": key_attrs,
                    "relaxation_history": relaxation_history,
                    "time_window": {
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                        "lookback_hours": lookback_hours
                    },
                    "generated_at": datetime.now(timezone.utc).isoformat()
                }
            }
            
        except Exception as e:
            logger.error(
                "query_generation_failed",
                error=str(e),
                incident_payload=incident_payload,
                exc_info=True
            )
            raise
    
    def _get_filter_from_llm(
        self, 
        incident_payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Use Claude via Bedrock to analyze payload and generate filter expressions.
        
        Args:
            incident_payload: The incident data
            
        Returns:
            Dictionary containing logs_filter, traces_filter, metrics_config, reasoning, key_attributes
        """
        user_prompt = get_query_generation_prompt(incident_payload)
        
        # Prepare Bedrock request body
        request_body = {
            "anthropic_version": self.anthropic_version,
            "max_tokens": 1500,  # Increased for multiple filters
            "temperature": 0.3,  # Lower temperature for more deterministic output
            "system": SYSTEM_PROMPT,
            "messages": [
                {
                    "role": "user",
                    "content": user_prompt
                }
            ]
        }
        
        try:
            # Invoke Bedrock model
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body),
                contentType="application/json",
                accept="application/json"
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            
            logger.debug(
                "bedrock_response_received",
                response_id=response_body.get('id'),
                stop_reason=response_body.get('stop_reason')
            )
            
            # Extract content from Claude's response
            content_blocks = response_body.get('content', [])
            if not content_blocks:
                raise Exception("Empty response from Bedrock")
            
            response_text = content_blocks[0].get('text', '').strip()
            logger.debug("llm_raw_response", response=response_text)
            
            # Parse JSON response
            result = json.loads(response_text)
            
            # Validate required fields
            if "logs_filter" not in result or "traces_filter" not in result:
                raise Exception("LLM response missing required filter fields")
            
            return {
                "logs_filter": result.get("logs_filter", ""),
                "traces_filter": result.get("traces_filter", ""),
                "metrics_config": result.get("metrics_config"),
                "reasoning": result.get("reasoning", ""),
                "key_attributes": result.get("key_attributes", [])
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(
                "bedrock_client_error",
                error_code=error_code,
                error_message=error_message
            )
            raise Exception(f"Bedrock API error ({error_code}): {error_message}")
            
        except json.JSONDecodeError as e:
            logger.error("llm_response_not_json", error=str(e), response=response_text)
            raise Exception(f"LLM returned invalid JSON: {e}")
            
        except KeyError as e:
            logger.error("llm_response_missing_field", error=str(e), result=result)
            raise Exception(f"LLM response missing required field: {e}")
            
        except Exception as e:
            logger.error("llm_api_error", error=str(e))
            raise
