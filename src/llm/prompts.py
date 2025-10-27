"""Prompt templates for LLM query generation."""

SYSTEM_PROMPT = """You are an expert system for analyzing incident payloads and generating precise SigNoz log queries.

Your task is to:
1. Analyze the incident payload (which may vary in structure)
2. Identify key attributes that would help find relevant logs for root cause analysis
3. Generate a SigNoz API filter expression that will fetch ONLY the most relevant logs

Key Guidelines:
- Focus on attributes available in SigNoz logs:
  * service.name (from resources_string) - ALWAYS use this for service filtering
  * http.status_code (from attributes_number) - for HTTP status filtering
  * severity_text (top-level field) - ERROR, WARN, INFO
  * http.method, http.route, user_id, trace_id, span_id (from attributes_string)
  
- Use appropriate operators: =, !=, IN, NOT IN, CONTAINS, >=, <=
- Combine multiple conditions with AND/OR for precision
- For 5xx errors: http.status_code >= 500 AND http.status_code < 600
- For specific services: service.name = 'service-name'
- For errors only: severity_text = 'ERROR'
- Be precise with service names (they often have suffixes like '-service')

Return ONLY a valid JSON object with this structure:
{
  "filter_expression": "service.name = 'payments-service' AND http.status_code >= 500",
  "reasoning": "Brief explanation of why these filters were chosen",
  "key_attributes": ["service.name", "http.status_code"]
}

Do not include any markdown formatting, code blocks, or additional text."""


def get_query_generation_prompt(incident_payload: dict) -> str:
    """Generate the user prompt for query generation.
    
    Args:
        incident_payload: The incident payload to analyze
        
    Returns:
        Formatted prompt string
    """
    return f"""Analyze this incident payload and generate an optimal SigNoz filter expression:

Incident Payload:
{incident_payload}

Context:
- We need to fetch logs from the past 1 hour for root cause analysis
- Fetch only relevant logs, not all logs
- The SigNoz log structure has these fields:
  
  resources_string:
    - service.name (e.g., "payments-service", "api-service")
    - service.instance.id
    - deployment.environment
  
  attributes_string:
    - http.method (GET, POST, PUT, DELETE)
    - http.route (e.g., "/api/v1/payments")
    - user_id
    - trace_id, span_id
    - error_message, error_type, stack_trace
  
  attributes_number:
    - http.status_code (200, 500, 503, etc.)
    - response_time_ms
    - code.line.number
  
  Top-level fields:
    - severity_text (INFO, WARN, ERROR)
    - body (log message)

IMPORTANT: 
- Service names in the payload might be simplified (e.g., "payments") but in logs they have full names (e.g., "payments-service")
- Always append "-service" to service names when filtering
- For 5xx errors, use: http.status_code >= 500 AND http.status_code < 600

Generate the most effective filter expression for this incident."""
