#!/usr/bin/env python3
"""Test 02: SigNoz Fetch - Verify logs are fetched and transformed correctly."""
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
from src.signoz.api_client import SigNozClient
from src.signoz.log_transformer import LogTransformer
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def load_test_payload():
    """Load test payload."""
    data_file = Path(__file__).parent.parent / "data" / "test_payloads.json"
    with open(data_file, 'r') as f:
        data = json.load(f)
    return data['test_incidents'][0]['payload']


def save_test_result(test_name, data):
    """Save test result."""
    reports_dir = Path(__file__).parent.parent.parent / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{test_name}_{timestamp}.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return str(filepath)


def main():
    """Test SigNoz fetch and transform."""
    print("\n" + "="*80)
    print("   TEST 02: SigNoz Fetch & Transform")
    print("="*80 + "\n")
    
    # Load test payload
    print(" Step 1: Loading test incident payload...")
    incident_payload = load_test_payload()
    print(f"   Incident ID: {incident_payload['incident_id']}\n")
    
    # Generate query
    print(" Step 2: Generating SigNoz query via LLM...")
    try:
        generator = QueryGenerator()
        query_result = generator.generate_signoz_query(incident_payload, lookback_hours=1)
        filter_expr = query_result['metadata']['filter_expression']
        print(f"    Query generated")
        print(f"   Filter: {filter_expr}\n")
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Fetch from SigNoz
    print(" Step 3: Fetching logs from SigNoz...")
    try:
        client = SigNozClient()
        raw_response = client.fetch_logs(
            query_payload=query_result['query'],
            incident_id=incident_payload['incident_id']
        )
        print(f"    Logs fetched from SigNoz\n")
    except Exception as e:
        print(f"   ❌ Failed: {e}\n")
        return False
    
    # Transform logs
    print(" Step 4: Transforming logs to expected format...")
    try:
        transformer = LogTransformer()
        transformed_logs = transformer.transform_logs(raw_response)
        print(f"    Transformed {len(transformed_logs)} logs\n")
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Display sample
    if transformed_logs:
        print(" Step 5: Sample Transformed Log:")
        print("-" * 80)
        sample = transformed_logs[0]
        print(f"   Timestamp: {sample.get('timestamp')}")
        print(f"   Service: {sample.get('service')}")
        print(f"   Level: {sample.get('level')}")
        print(f"   Status Code: {sample.get('status_code')}")
        print(f"   Message: {sample.get('message', '')[:60]}...\n")
    else:
        print("  No logs found (this might be expected if no matching logs exist)\n")
    
    # Prepare data flow
    data_flow = {
        "test_name": "02_signoz_fetch_transform",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "input": {
            "incident_id": incident_payload['incident_id'],
            "filter_expression": filter_expr,
            "signoz_query": query_result['query']
        },
        "output": {
            "raw_response_size": len(json.dumps(raw_response)),
            "logs_fetched": len(transformed_logs),
            "sample_log": transformed_logs[0] if transformed_logs else None,
            "transformed_logs": transformed_logs[:5]  # Save first 5
        },
        "signoz_info": {
            "endpoint": client.api_endpoint,
            "api_version": "v5"
        }
    }
    
    # Save result
    print(" Step 6: Saving test results...")
    filepath = save_test_result("02_signoz_fetch", data_flow)
    print(f"    Saved to: {filepath}\n")

    print("="*80)
    print("   TEST 02 PASSED")
    print("="*80 + "\n")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
