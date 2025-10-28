"""Test LLM Query Generation - Verify Bedrock generates correct SigNoz queries."""
import pytest
import json
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

from src.llm.query_generator import QueryGenerator
from src.utils.logger import setup_logging, get_logger

# Load environment variables
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


def save_test_result(reports_dir, test_name, data):
    """Save test result to reports directory."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{test_name}_{timestamp}.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return str(filepath)


def test_llm_query_generation(query_generator, test_payload, reports_dir):
    """Test that LLM generates valid SigNoz query from incident payload."""
    
    print("\n" + "="*80)
    print("  TEST: LLM Query Generation")
    print("="*80 + "\n")
    
    # Step 1: Display input
    print("Step 1: Input Incident Payload")
    print(f"   Incident ID: {test_payload['incident_id']}")
    print(f"   Service: {test_payload['service']['name']}")
    print(f"   Title: {test_payload['title']}\n")
    
    # Step 2: Generate query
    print("Step 2: Generating SigNoz query with Bedrock Claude...")
    result = query_generator.generate_signoz_query(
        incident_payload=test_payload,
        lookback_hours=1
    )
    print("   ✓ Query generated successfully\n")
    
    # Step 3: Validate structure
    print("Step 3: Validating query structure...")
    assert result is not None, "Result should not be None"
    assert 'query' in result, "Result should contain 'query'"
    assert 'metadata' in result, "Result should contain 'metadata'"
    
    query = result['query']
    metadata = result['metadata']
    
    # Validate query fields
    assert 'start' in query, "Query should have 'start' timestamp"
    assert 'end' in query, "Query should have 'end' timestamp"
    assert 'compositeQuery' in query, "Query should have 'compositeQuery'"
    assert query['requestType'] == 'raw', "requestType should be 'raw'"
    
    # Validate metadata
    assert 'filter_expression' in metadata, "Metadata should have 'filter_expression'"
    assert 'reasoning' in metadata, "Metadata should have 'reasoning'"
    assert 'key_attributes' in metadata, "Metadata should have 'key_attributes'"
    assert len(metadata['filter_expression']) > 0, "Filter expression should not be empty"
    print("   ✓ All validations passed\n")
    
    # Step 4: Display results
    print("Step 4: Query Details")
    print("-" * 80)
    print(f"Filter Expression:")
    print(f"   {metadata['filter_expression']}\n")
    print(f"Reasoning:")
    print(f"   {metadata['reasoning']}\n")
    print(f"Key Attributes:")
    print(f"   {', '.join(metadata['key_attributes'])}\n")
    
    # Step 5: Prepare and save report
    print("Step 5: Saving test results...")
    data_flow = {
        "test_name": "llm_query_generation",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        
        # Input
        "input": {
            "incident_payload": test_payload,
            "lookback_hours": 1
        },
        
        # LLM Output
        "llm_output": {
            "filter_expression": metadata['filter_expression'],
            "reasoning": metadata['reasoning'],
            "key_attributes": metadata['key_attributes']
        },
        
        # Complete SigNoz Query
        "signoz_query": query,
        
        # Query Details
        "query_details": {
            "time_range": {
                "start_ms": query['start'],
                "end_ms": query['end'],
                "start_human": datetime.fromtimestamp(query['start']/1000, tz=timezone.utc).isoformat(),
                "end_human": datetime.fromtimestamp(query['end']/1000, tz=timezone.utc).isoformat(),
                "duration_hours": (query['end'] - query['start']) / (1000 * 60 * 60)
            },
            "request_type": query['requestType'],
            "num_queries": len(query['compositeQuery']['queries'])
        },
        
        # LLM Info
        "llm_info": {
            "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
            "service": "AWS Bedrock"
        }
    }
    
    filepath = save_test_result(reports_dir, "llm_query", data_flow)
    print(f"   ✓ Saved to: {filepath}\n")
    
    print("="*80)
    print("   TEST PASSED ✓")
    print("="*80 + "\n")


def test_query_structure_validity(query_generator, test_payload):
    """Test that generated query has valid SigNoz structure."""
    
    result = query_generator.generate_signoz_query(test_payload, lookback_hours=1)
    query = result['query']
    
    # Validate composite query structure
    assert 'compositeQuery' in query
    assert 'queries' in query['compositeQuery']
    assert len(query['compositeQuery']['queries']) > 0
    
    # Validate first query spec
    first_query = query['compositeQuery']['queries'][0]
    assert 'spec' in first_query
    assert 'filter' in first_query['spec']
    assert 'expression' in first_query['spec']['filter']
    
    logger.info("query_structure_valid")


def test_time_range_calculation(query_generator, test_payload):
    """Test that time range is correctly calculated."""
    
    lookback_hours = 2
    result = query_generator.generate_signoz_query(test_payload, lookback_hours=lookback_hours)
    query = result['query']
    
    # Calculate time difference
    time_diff_ms = query['end'] - query['start']
    expected_diff_ms = lookback_hours * 60 * 60 * 1000
    
    # Allow 5 second tolerance
    tolerance_ms = 5000
    assert abs(time_diff_ms - expected_diff_ms) < tolerance_ms
    
    logger.info("time_range_valid", lookback_hours=lookback_hours)
