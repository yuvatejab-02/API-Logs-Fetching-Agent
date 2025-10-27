"""LLM-based SigNoz query generator using AWS Bedrock."""
import json
from typing import Dict, Any, Tuple
from datetime import datetime, timedelta
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import time

from ..utils.config import get_settings
from ..utils.logger import get_logger
from .prompts import SYSTEM_PROMPT, get_query_generation_prompt

logger = get_logger(__name__)


class QueryGenerator:
    """Generates SigNoz API queries using Claude via AWS Bedrock."""
    
    def __init__(self):
        """Initialize the query generator with Bedrock client."""
        settings = get_settings()
        
        # Configure boto3 client with extended timeout for Bedrock
        bedrock_config = Config(
            region_name=settings.aws_region,
            read_timeout=300,  # 5 minutes
            connect_timeout=60,
            retries={'max_attempts': 3, 'mode': 'standard'}
        )
        
        # Initialize Bedrock Runtime client
        self.client = boto3.client(
            service_name='bedrock-runtime',
            region_name=settings.aws_region,
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
            region=settings.aws_region
        )
        
    def generate_signoz_query(
        self, 
        incident_payload: Dict[str, Any],
        lookback_hours: int = 1
    ) -> Dict[str, Any]:
        """Generate a SigNoz API query from incident payload.
        
        Args:
            incident_payload: The incident data
            lookback_hours: How many hours back to search (default: 1)
            
        Returns:
            Complete SigNoz API query payload
            
        Raises:
            Exception: If LLM fails to generate valid query
        """
        logger.info(
            "generating_signoz_query",
            incident_id=incident_payload.get("incident_id"),
            lookback_hours=lookback_hours
        )
        
        try:
            # Get filter expression from LLM
            filter_expression, reasoning, key_attrs = self._get_filter_from_llm(
                incident_payload
            )
            
            # Build complete SigNoz query
            signoz_query = self._build_signoz_payload(
                filter_expression=filter_expression,
                lookback_hours=lookback_hours
            )
            
            logger.info(
                "query_generated_successfully",
                filter_expression=filter_expression,
                reasoning=reasoning,
                key_attributes=key_attrs
            )
            
            return {
                "query": signoz_query,
                "metadata": {
                    "filter_expression": filter_expression,
                    "reasoning": reasoning,
                    "key_attributes": key_attrs,
                    "generated_at": datetime.utcnow().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(
                "query_generation_failed",
                error=str(e),
                incident_payload=incident_payload
            )
            raise
    
    def _get_filter_from_llm(
        self, 
        incident_payload: Dict[str, Any]
    ) -> Tuple[str, str, list]:
        """Use Claude via Bedrock to analyze payload and generate filter expression.
        
        Args:
            incident_payload: The incident data
            
        Returns:
            Tuple of (filter_expression, reasoning, key_attributes)
        """
        user_prompt = get_query_generation_prompt(incident_payload)
        
        # Prepare Bedrock request body
        request_body = {
            "anthropic_version": self.anthropic_version,
            "max_tokens": 1024,
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
            
            return (
                result["filter_expression"],
                result["reasoning"],
                result["key_attributes"]
            )
            
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
    
    def _build_signoz_payload(
        self, 
        filter_expression: str,
        lookback_hours: int = 1,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Build complete SigNoz API payload.
        
        Args:
            filter_expression: The filter expression from LLM
            lookback_hours: Hours to look back
            limit: Maximum logs to fetch
            
        Returns:
            Complete SigNoz API payload
        """
        
        # Convert to epoch milliseconds
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=lookback_hours)
        
        return {
            "start": int(start_time.timestamp() * 1000),
            "end": int(end_time.timestamp() * 1000),
            "requestType": "raw",
            "variables": {},
            "compositeQuery": {
                "queries": [
                    {
                        "type": "builder_query",
                        "spec": {
                            "name": "A",
                            "signal": "logs",
                            "filter": {
                                "expression": filter_expression
                            },
                            "order": [
                                {
                                    "key": {"name": "timestamp"},
                                    "direction": "desc"
                                },
                                {
                                    "key": {"name": "id"},
                                    "direction": "desc"
                                }
                            ],
                            "offset": 0,
                            "limit": limit
                        }
                    }
                ]
            }
        }
