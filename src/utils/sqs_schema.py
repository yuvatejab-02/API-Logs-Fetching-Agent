"""SQS message schema definitions and validation."""
import fastjsonschema
from typing import Dict, Any

from ..utils.logger import get_logger

logger = get_logger(__name__)


# JSON Schema for incident payload
INCIDENT_PAYLOAD_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["incident_id", "service"],
    "properties": {
        "compslug": {
            "type": "string",
            "description": "Company slug identifier"
        },
        "Session_id": {
            "type": "string",
            "description": "Session identifier"
        },
        "incident_id": {
            "type": "string",
            "minLength": 1,
            "description": "Unique incident identifier"
        },
        "title": {
            "type": "string",
            "description": "Incident title"
        },
        "service": {
            "type": "object",
            "required": ["name"],
            "properties": {
                "id": {
                    "type": "string",
                    "description": "Service ID"
                },
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Service name"
                }
            }
        },
        "environment": {
            "type": "string",
            "enum": ["prod", "staging", "dev"],
            "description": "Environment where incident occurred"
        },
        "tenant": {
            "type": "string",
            "description": "Tenant identifier"
        },
        "lookback_hours": {
            "type": "integer",
            "minimum": 1,
            "maximum": 24,
            "description": "Hours to look back for data"
        },
        "severity": {
            "type": "string",
            "enum": ["critical", "high", "medium", "low"],
            "description": "Incident severity"
        },
        "description": {
            "type": "string",
            "description": "Incident description"
        },
        "metadata": {
            "type": "object",
            "description": "Additional metadata"
        }
    }
}

# Compile schema for fast validation
validate_incident_payload = fastjsonschema.compile(INCIDENT_PAYLOAD_SCHEMA)


# JSON Schema for job completion message
JOB_COMPLETION_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["incident_id", "status", "timestamp"],
    "properties": {
        "incident_id": {
            "type": "string",
            "minLength": 1
        },
        "status": {
            "type": "string",
            "enum": ["completed", "failed", "partial"]
        },
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "processing_time_seconds": {
            "type": "number",
            "minimum": 0
        },
        "signals_fetched": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["logs", "traces", "metrics"]
            }
        },
        "s3_keys": {
            "type": "object",
            "properties": {
                "logs": {"type": "string"},
                "traces": {"type": "string"},
                "metrics": {"type": "string"}
            }
        },
        "edal_descriptor_key": {
            "type": "string"
        },
        "error_message": {
            "type": "string"
        },
        "metadata": {
            "type": "object"
        }
    }
}

# Compile schema for fast validation
validate_job_completion = fastjsonschema.compile(JOB_COMPLETION_SCHEMA)


def validate_sqs_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Validate SQS message payload.
    
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
        
        logger.info(
            "sqs_message_validated",
            incident_id=message.get("incident_id"),
            service=message.get("service", {}).get("name")
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
    status: str,
    timestamp: str,
    processing_time: float,
    signals_fetched: list,
    s3_keys: Dict[str, str],
    edal_descriptor_key: str = None,
    error_message: str = None,
    metadata: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Create a job completion message.
    
    Args:
        incident_id: Incident identifier
        status: Job status (completed, failed, partial)
        timestamp: ISO format timestamp
        processing_time: Processing time in seconds
        signals_fetched: List of signals fetched
        s3_keys: Dictionary of S3 keys for each signal
        edal_descriptor_key: Optional EDAL descriptor S3 key
        error_message: Optional error message
        metadata: Optional additional metadata
        
    Returns:
        Validated job completion message
    """
    message = {
        "incident_id": incident_id,
        "status": status,
        "timestamp": timestamp,
        "processing_time_seconds": round(processing_time, 2),
        "signals_fetched": signals_fetched,
        "s3_keys": s3_keys
    }
    
    if edal_descriptor_key:
        message["edal_descriptor_key"] = edal_descriptor_key
    
    if error_message:
        message["error_message"] = error_message
    
    if metadata:
        message["metadata"] = metadata
    
    # Validate before returning
    validate_job_completion(message)
    
    return message


