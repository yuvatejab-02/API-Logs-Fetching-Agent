"""SQS message schema definitions and validation."""
import fastjsonschema
from typing import Dict, Any, Tuple

from ..utils.logger import get_logger

logger = get_logger(__name__)


# JSON Schema for NEW incident payload format
INCIDENT_PAYLOAD_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["job_type", "data_sources", "incident"],
    "properties": {
        "job_type": {
            "type": "string",
            "enum": ["incident-data-fetch"]
        },
        "data_sources": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1,
            "items": {
                "type": "object",
                "required": ["source_type", "connection_info", "auth_config"],
                "properties": {
                    "source_type": {
                        "type": "string",
                        "enum": ["signoz"]
                    },
                    "connection_info": {
                        "type": "object",
                        "required": ["api_endpoint"],
                        "properties": {
                            "api_endpoint": {
                                "type": "string",
                                "pattern": "^https://[a-z0-9-]+\\.[a-z]+\\.signoz\\.cloud$"
                            }
                        },
                        "additionalProperties": False
                    },
                    "auth_config": {
                        "type": "object",
                        "required": ["auth_type", "api_key"],
                        "properties": {
                            "auth_type": {
                                "type": "string",
                                "enum": ["api_key"]
                            },
                            "api_key": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "additionalProperties": False
                    }
                },
                "additionalProperties": False
            }
        },
        "incident": {
            "type": "object",
            "required": ["incident_id", "company_id", "title", "service"],
            "properties": {
                "incident_id": {
                    "type": "string",
                    "pattern": "^INC_[a-zA-Z0-9_]+$",
                    "minLength": 1
                },
                "company_id": {
                    "type": "string",
                    "pattern": "^[a-z0-9-]+$",
                    "minLength": 1
                },
                "title": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 500
                },
                "service": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {
                            "type": "string",
                            "pattern": "^[a-z0-9-]+$",
                            "minLength": 1
                        }
                    },
                    "additionalProperties": False
                },
                "environment": {
                    "type": "string",
                    "enum": ["prod", "staging", "dev"]
                },
                "lookback_hours": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 24
                }
            },
            "additionalProperties": False
        }
    },
    "additionalProperties": True  # Allow _sqs_metadata and other internal fields
}

# Compile schema for fast validation
validate_incident_payload = fastjsonschema.compile(INCIDENT_PAYLOAD_SCHEMA)


# JSON Schema for job completion message (new format)
JOB_COMPLETION_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["incident", "sources"],
    "properties": {
        "incident": {
            "type": "object",
            "required": ["incident_id", "company_id", "service", "env"],
            "properties": {
                "incident_id": {
                    "type": "string",
                    "pattern": "^INC_[a-zA-Z0-9_]+$",
                    "minLength": 1
                },
                "company_id": {
                    "type": "string",
                    "minLength": 1
                },
                "service": {
                    "type": "string",
                    "minLength": 1
                },
                "env": {
                    "type": "string",
                    "enum": ["dev", "staging", "prod", "production"]
                }
            },
            "additionalProperties": False
        },
        "sources": {
            "type": "object",
            "properties": {
                "signoz": {
                    "type": "object",
                    "properties": {
                        "logs": {"type": "string"},
                        "traces": {"type": "string"},
                        "metrics": {"type": "string"}
                    },
                    "additionalProperties": False
                }
            },
            "additionalProperties": False
        }
    },
    "additionalProperties": False
}

# Compile schema for fast validation
validate_job_completion = fastjsonschema.compile(JOB_COMPLETION_SCHEMA)


def validate_and_extract_payload(message: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    """Validate SQS message payload and extract SigNoz credentials.
    
    Args:
        message: Raw message from SQS
        
    Returns:
        Tuple of (incident_data, signoz_api_endpoint, signoz_api_key)
        
    Raises:
        fastjsonschema.JsonSchemaException: If validation fails
        ValueError: If data extraction fails
    """
    try:
        # Validate against schema
        validate_incident_payload(message)
        
        # Extract data sources (should be exactly 1 SigNoz source)
        data_sources = message.get("data_sources", [])
        if not data_sources or len(data_sources) != 1:
            raise ValueError("Expected exactly 1 data source in payload")
        
        data_source = data_sources[0]
        
        # Validate source type
        if data_source.get("source_type") != "signoz":
            raise ValueError(f"Unsupported source_type: {data_source.get('source_type')}. Only 'signoz' is supported.")
        
        # Extract SigNoz credentials
        connection_info = data_source.get("connection_info", {})
        auth_config = data_source.get("auth_config", {})
        
        signoz_api_endpoint = connection_info.get("api_endpoint")
        signoz_api_key = auth_config.get("api_key")
        
        if not signoz_api_endpoint:
            raise ValueError("Missing api_endpoint in connection_info")
        
        if not signoz_api_key:
            raise ValueError("Missing api_key in auth_config")
        
        # Extract incident data
        incident = message.get("incident", {})
        incident_id = incident.get("incident_id")
        company_id = incident.get("company_id")
        
        if not incident_id:
            raise ValueError("Missing incident_id in incident section")
        
        if not company_id:
            raise ValueError("Missing company_id in incident section")
        
        logger.info(
            "payload_validated_and_extracted",
            incident_id=incident_id,
            company_id=company_id,
            service=incident.get("service", {}).get("name"),
            signoz_endpoint=signoz_api_endpoint,
            has_api_key=bool(signoz_api_key)
        )
        
        return incident, signoz_api_endpoint, signoz_api_key
        
    except fastjsonschema.JsonSchemaException as e:
        logger.error(
            "payload_validation_failed",
            error=str(e),
            message_keys=list(message.keys()) if isinstance(message, dict) else None
        )
        raise
    
    except ValueError as e:
        logger.error(
            "payload_extraction_failed",
            error=str(e),
            message_keys=list(message.keys()) if isinstance(message, dict) else None
        )
        raise
    
    except Exception as e:
        logger.error(
            "unexpected_validation_error",
            error=str(e),
            error_type=type(e).__name__
        )
        raise ValueError(f"Failed to validate and extract payload: {str(e)}")


def validate_sqs_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Validate SQS message payload (legacy method for backward compatibility).
    
    Args:
        message: Raw message from SQS
        
    Returns:
        Validated incident payload
        
    Raises:
        fastjsonschema.JsonSchemaException: If validation fails
    """
    try:
        # Validate against schema
        validate_incident_payload(message)
        
        incident = message.get("incident", {})
        incident_id = incident.get("incident_id")
        
        logger.info(
            "sqs_message_validated",
            incident_id=incident_id,
            service=incident.get("service", {}).get("name")
        )
        
        return message
        
    except fastjsonschema.JsonSchemaException as e:
        logger.error(
            "sqs_message_validation_failed",
            error=str(e),
            message_keys=list(message.keys())
        )
        raise


def create_job_completion_message(
    incident_id: str,
    company_id: str,
    service: str,
    environment: str,
    s3_urls: Dict[str, str]
) -> Dict[str, Any]:
    """Create a job completion message in the new format.
    
    Args:
        incident_id: Incident identifier
        company_id: Company/tenant identifier
        service: Service name
        environment: Environment (prod, dev, staging)
        s3_urls: Dictionary of S3 URLs for each signal (logs, traces, metrics)
        
    Returns:
        Job completion message in new format
    """
    message = {
        "incident": {
            "incident_id": incident_id,
            "company_id": company_id,
            "service": service,
            "env": environment
        },
        "sources": {
            "signoz": {}
        }
    }
    
    # Add S3 URLs for each signal that was fetched
    if "logs" in s3_urls and s3_urls["logs"]:
        message["sources"]["signoz"]["logs"] = s3_urls["logs"]
    
    if "traces" in s3_urls and s3_urls["traces"]:
        message["sources"]["signoz"]["traces"] = s3_urls["traces"]
    
    if "metrics" in s3_urls and s3_urls["metrics"]:
        message["sources"]["signoz"]["metrics"] = s3_urls["metrics"]
    
    # Validate before returning
    validate_job_completion(message)
    
    return message
