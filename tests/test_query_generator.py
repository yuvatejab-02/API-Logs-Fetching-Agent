"""Tests for LLM query generator."""
import json
import pytest
from pathlib import Path
from src.llm.query_generator import QueryGenerator
from src.utils.logger import setup_logging, get_logger

# Setup logging for tests
setup_logging()
logger = get_logger(__name__)


def load_test_payloads():
    """Load sample test payloads."""
    test_data_path = Path(__file__).parent / "test_data" / "sample_payloads.json"
    with open(test_data_path, 'r') as f:
        return json.load(f)["test_cases"]


@pytest.fixture
def query_generator():
    """Create QueryGenerator instance."""
    return QueryGenerator()


@pytest.mark.parametrize("test_case", load_test_payloads())
def test_query_generation(query_generator, test_case):
    """Test query generation for various incident payloads."""
    payload = test_case["payload"]
    test_name = test_case["name"]
    
    logger.info(f"Testing: {test_name}")
    
    # Generate query
    result = query_generator.generate_signoz_query(payload, lookback_hours=1)
    
    # Assertions
    assert "query" in result
    assert "metadata" in result
    
    query = result["query"]
    metadata = result["metadata"]
    
    # Validate query structure
    assert "start" in query
    assert "end" in query
    assert "compositeQuery" in query
    assert query["requestType"] == "raw"
    
    # Validate filter expression exists
    filter_expr = query["compositeQuery"]["queries"][0]["spec"]["filter"]["expression"]
    assert len(filter_expr) > 0
    
    # Validate metadata
    assert "filter_expression" in metadata
    assert "reasoning" in metadata
    assert "key_attributes" in metadata
    
    logger.info(
        f"✓ Test passed: {test_name}",
        filter_expression=metadata["filter_expression"],
        reasoning=metadata["reasoning"]
    )
    
    # Print for manual review
    print(f"\n{'='*80}")
    print(f"Test Case: {test_name}")
    print(f"{'='*80}")
    print(f"Filter Expression: {metadata['filter_expression']}")
    print(f"Reasoning: {metadata['reasoning']}")
    print(f"Key Attributes: {metadata['key_attributes']}")
    print(f"{'='*80}\n")


def test_manual_payload(query_generator):
    """Test with the exact payload from your example."""
    payload = {
        "compslug": "companyname",
        "Session_id": "7b3f2f30-0c4b-4c42-9f8f-7e3d2b8e2a61",
        "incident_id": "INC_9d0f2a3a",
        "title": "Spike in 5xx for payments",
        "service": {
            "id": "al89asf9asdhjfaslkdjfl",
            "name": "payments-service"
        }
    }
    
    result = query_generator.generate_signoz_query(payload)
    
    # Print complete result for inspection
    print("\n" + "="*80)
    print("COMPLETE GENERATED QUERY")
    print("="*80)
    print(json.dumps(result, indent=2))
    print("="*80 + "\n")
    
    assert result is not None
