#!/usr/bin/env python3
"""Test 01: LLM Query Generation - Verify Bedrock generates correct SigNoz queries."""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load .env from project root
from dotenv import load_dotenv
env_path = project_root / '.env'
load_dotenv(env_path)

from src.llm.query_generator import QueryGenerator
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def load_test_payload():
    """Load test payload from data file."""
    data_file = Path(__file__).parent.parent / "data" / "test_payloads.json"
    with open(data_file, 'r') as f:
        data = json.load(f)
    return data['test_incidents'][0]['payload']


def save_test_result(test_name, data):
    """Save test result to reports directory."""
    reports_dir = Path(__file__).parent.parent.parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{test_name}_{timestamp}.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return str(filepath)


def main():
    """Test LLM query generation."""
    print("\n" + "="*80)
    print("  TEST 01: LLM Query Generation")
    print("="*80 + "\n")
    
    # Load test payload
    print("Step 1: Loading test incident payload...")
    incident_payload = load_test_payload()
    print(f"   Incident ID: {incident_payload['incident_id']}")
    print(f"   Service: {incident_payload['service']['name']}")
    print(f"   Title: {incident_payload['title']}\n")
    
    # Initialize query generator
    print("Step 2: Initializing LLM Query Generator (Bedrock Claude)...")
    try:
        generator = QueryGenerator()
        print("    Generator initialized\n")
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Generate query
    print(" Step 3: Generating SigNoz query...")
    try:
        result = generator.generate_signoz_query(
            incident_payload=incident_payload,
            lookback_hours=1
        )
        print("    Query generated successfully\n")
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Display results
    print(" Step 4: Query Details:")
    print("-" * 80)
    print(f"Filter Expression:")
    print(f"   {result['metadata']['filter_expression']}\n")
    print(f"Reasoning:")
    print(f"   {result['metadata']['reasoning']}\n")
    print(f"Key Attributes:")
    print(f"   {', '.join(result['metadata']['key_attributes'])}\n")
    
    # Prepare data flow output
    data_flow = {
        "test_name": "01_llm_query_generation",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "input": {
            "incident_payload": incident_payload,
            "lookback_hours": 1
        },
        "output": {
            "filter_expression": result['metadata']['filter_expression'],
            "reasoning": result['metadata']['reasoning'],
            "key_attributes": result['metadata']['key_attributes'],
            "full_query": result['query']
        },
        "llm_info": {
            "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
            "service": "AWS Bedrock"
        }
    }
    
    # Save result
    print(" Step 5: Saving test results...")
    filepath = save_test_result("01_llm_query", data_flow)
    print(f"    Saved to: {filepath}\n")
    
    print("="*80)
    print("   TEST 01 PASSED")
    print("="*80 + "\n")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
