"""Test SigNoz log fetching and transformation."""
import pytest
import json
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

from src.llm.query_generator import QueryGenerator
from src.signoz.api_client import SigNozClient
from src.signoz.log_transformer import LogTransformer
from src.utils.logger import setup_logging, get_logger

# Load environment
load_dotenv()

# Setup logging
setup_logging()
logger = get_logger(__name__)


@pytest.fixture(scope="session")
def reports_dir():
    """Create and return reports directory."""
    reports = Path(__file__).parent / "reports"
    reports.mkdir(exist_ok=True)
    return reports


@pytest.fixture
def test_payload():
    """Load test incident payload."""
    data_file = Path(__file__).parent / "test_data" / "test_payloads.json"
    with open(data_file, 'r') as f:
        data = json.load(f)
    return data['test_incidents'][0]['payload']


@pytest.fixture
def query_generator():
    """Initialize LLM Query Generator."""
    return QueryGenerator()


@pytest.fixture
def signoz_client():
    """Initialize SigNoz API client."""
    return SigNozClient()


def save_test_result(reports_dir, test_name, data):
    """Save test result to reports directory."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{test_name}_{timestamp}.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return str(filepath)


def test_signoz_connection(signoz_client):
    """Test that we can connect to SigNoz API."""
    
    print("\n" + "="*80)
    print("  TEST: SigNoz Connection")
    print("="*80 + "\n")
    
    print("Step 1: Testing SigNoz API connection...")
    result = signoz_client.test_connection()
    
    assert result is True, "SigNoz connection should succeed"
    print("   ✓ Connection successful\n")
    
    print("="*80)
    print("   TEST PASSED ✓")
    print("="*80 + "\n")


def test_signoz_fetch_and_transform(
    query_generator, 
    signoz_client,
    test_payload, 
    reports_dir
):
    """Test complete flow: Generate query → Fetch logs → Transform logs."""
    
    print("\n" + "="*80)
    print("  TEST: SigNoz Fetch & Transform")
    print("="*80 + "\n")
    
    # Step 1: Display input
    print("Step 1: Input Incident Payload")
    print(f"   Incident ID: {test_payload['incident_id']}")
    print(f"   Service: {test_payload['service']['name']}")
    print(f"   Title: {test_payload['title']}\n")
    
    # Step 2: Generate SigNoz query via LLM
    print("Step 2: Generating SigNoz query via LLM...")
    query_result = query_generator.generate_signoz_query(
        incident_payload=test_payload,
        lookback_hours=1
    )
    
    filter_expr = query_result['metadata']['filter_expression']
    signoz_query = query_result['query']
    
    print(f"   ✓ Query generated")
    print(f"   Filter: {filter_expr}\n")
    
    # Step 3: Fetch logs from SigNoz
    print("Step 3: Fetching logs from SigNoz API...")
    try:
        raw_response = signoz_client.fetch_logs(
            query_payload=signoz_query,
            incident_id=test_payload['incident_id']
        )
        
        assert raw_response is not None, "Response should not be None"
        raw_response_size = len(json.dumps(raw_response))
        print(f"   ✓ Logs fetched from SigNoz")
        print(f"   Raw response size: {raw_response_size:,} bytes\n")
        
    except Exception as e:
        pytest.fail(f"Log fetch failed: {e}")
    
    # Step 4: Transform logs using LogTransformer
    print("Step 4: Transforming logs...")
    try:
        transformed_logs = LogTransformer.transform_logs(raw_response)
        
        assert transformed_logs is not None, "Transformed logs should not be None"
        assert isinstance(transformed_logs, list), "Transformed logs should be a list"
        
        print(f"   ✓ Logs transformed successfully")
        print(f"   Total logs: {len(transformed_logs)}\n")
        
    except Exception as e:
        pytest.fail(f"Log transformation failed: {e}")
    
    # Step 5: Display sample log (if any)
    if transformed_logs:
        print("Step 5: Sample Transformed Log")
        print("-" * 80)
        sample = transformed_logs[0]
        print(f"   Timestamp: {sample.get('timestamp')}")
        print(f"   Service: {sample.get('service')}")
        print(f"   Level: {sample.get('level')}")
        print(f"   Status Code: {sample.get('status_code', 'N/A')}")
        print(f"   Request ID: {sample.get('request_id', 'N/A')}")
        
        message = sample.get('message', '')
        if message:
            print(f"   Message: {message[:100]}{'...' if len(message) > 100 else ''}\n")
        else:
            print(f"   Message: (empty)\n")
        
        # Show first 5 for verification
        print(f"   First 5 logs previewed in report\n")
    else:
        print("Step 5: No logs found")
        print("   ⚠ This may be expected if no logs match the filter expression\n")
    
    # Step 6: Validate log structure (if logs exist)
    if transformed_logs:
        print("Step 6: Validating transformed log structure...")
        sample = transformed_logs[0]
        
        # Check for essential fields
        assert 'timestamp' in sample, "Log should have 'timestamp' field"
        assert 'service' in sample, "Log should have 'service' field"
        assert 'level' in sample, "Log should have 'level' field"
        
        print("   ✓ All essential fields present\n")
    else:
        print("Step 6: Skipping validation (no logs to validate)\n")
    
    # Step 7: Save report in EXACT format you specified
    print("Step 7: Saving test results...")
    
    # Build report in exact format
    report = {
        "test_name": "02_signoz_fetch_transform",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        
        # Input section
        "input": {
            "incident_id": test_payload['incident_id'],
            "filter_expression": filter_expr,
            "signoz_query": signoz_query
        },
        
        # Output section
        "output": {
            "raw_response_size": raw_response_size,
            "logs_fetched": len(transformed_logs),
            "sample_log": transformed_logs[0] if transformed_logs else None,
            "transformed_logs": transformed_logs[:5] if len(transformed_logs) >= 5 else transformed_logs
        },
        
        # SigNoz info
        "signoz_info": {
            "endpoint": signoz_client.api_endpoint,
            "api_version": "v5"
        }
    }
    
    filepath = save_test_result(reports_dir, "02_signoz_fetch_transform", report)
    print(f"   ✓ Saved to: {filepath}\n")
    
    # Also save full transformed logs separately
    if transformed_logs:
        full_logs_file = reports_dir / f"transformed_logs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(full_logs_file, 'w', encoding='utf-8') as f:
            json.dump({
                "incident_id": test_payload['incident_id'],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_logs": len(transformed_logs),
                "logs": transformed_logs
            }, f, indent=2, ensure_ascii=False)
        print(f"   ✓ Full transformed logs saved to: {full_logs_file}\n")
    
    print("="*80)
    print("   TEST PASSED ✓")
    print("="*80 + "\n")


def test_log_transformation_structure():
    """Test that log transformer produces correct structure with mock data."""
    
    # Mock SigNoz v5 response structure
    mock_response = {
        "data": {
            "data": {
                "results": [
                    {
                        "rows": [
                            {
                                "timestamp": "2025-10-28T10:00:00Z",
                                "data": {
                                    "severity_text": "ERROR",
                                    "body": "GET /api/v1/users - 503 (65ms)",
                                    "attributes_string": {
                                        "http.method": "GET",
                                        "http.route": "/api/v1/users",
                                        "trace_id": "abc123def456",
                                        "user_id": "user-1234"
                                    },
                                    "attributes_number": {
                                        "http.status_code": 503,
                                        "response_time_ms": 65
                                    },
                                    "resources_string": {
                                        "service.name": "payments-service",
                                        "service.instance.id": "payments-service-xyz789",
                                        "deployment.environment": "testing"
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
        }
    }
    
    # Transform using static method
    transformed = LogTransformer.transform_logs(mock_response)
    
    # Validate structure matches expected format
    assert len(transformed) == 1, "Should transform 1 log"
    
    log = transformed[0]
    
    # Validate all expected fields
    assert log['timestamp'] == "2025-10-28T10:00:00Z"
    assert log['service'] == "payments-service"
    assert log['instance_id'] == "payments-service-xyz789"
    assert log['level'] == "ERROR"
    assert log['request_id'] == "abc123def456"
    assert log['company_id'] == "testing"
    assert log['user_id'] == "user-1234"
    assert log['method'] == "GET"
    assert log['path'] == "/api/v1/users"
    assert log['status_code'] == 503
    assert log['response_time_ms'] == 65
    assert log['message'] == "GET /api/v1/users - 503 (65ms)"
    
    logger.info("log_transformation_structure_valid")
    
    print("\n✓ Transformed log structure matches expected format")


def test_empty_response_handling():
    """Test that transformer handles empty responses gracefully."""
    
    # Empty response
    empty_response = {
        "data": {
            "data": {
                "results": []
            }
        }
    }
    
    transformed = LogTransformer.transform_logs(empty_response)
    
    assert transformed == [], "Should return empty list for empty response"
    
    logger.info("empty_response_handled")
