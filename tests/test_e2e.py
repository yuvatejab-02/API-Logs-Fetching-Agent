"""End-to-End Integration Test - Complete workflow."""
import pytest
import json
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

from src.llm.query_generator import QueryGenerator
from src.signoz.api_client import SigNozClient
from src.signoz.log_transformer import LogTransformer
from src.storage.s3_storage import S3Storage
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


@pytest.fixture
def s3_storage():
    """Initialize S3 Storage."""
    return S3Storage()


def save_test_result(reports_dir, test_name, data):
    """Save test result to reports directory."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{test_name}_{timestamp}.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return str(filepath)


def test_complete_incident_log_workflow(
    query_generator,
    signoz_client,
    s3_storage,
    test_payload,
    reports_dir
):
    """Test complete end-to-end workflow: Incident → Query → Fetch → Transform → Store."""
    
    print("\n" + "="*80)
    print("  END-TO-END TEST: Complete Incident Log Analysis Workflow")
    print("="*80 + "\n")
    
    incident_id = test_payload['incident_id']
    
    # ============================================================================
    # STEP 1: Input Incident Payload
    # ============================================================================
    print("STEP 1: Incident Payload Received")
    print("-" * 80)
    print(f"   Incident ID: {incident_id}")
    print(f"   Service: {test_payload['service']['name']}")
    print(f"   Title: {test_payload['title']}")
    print(f"   Company: {test_payload.get('compslug', 'N/A')}\n")
    
    # ============================================================================
    # STEP 2: Generate SigNoz Query via LLM
    # ============================================================================
    print("STEP 2: LLM Query Generation (AWS Bedrock Claude)")
    print("-" * 80)
    
    try:
        query_result = query_generator.generate_signoz_query(
            incident_payload=test_payload,
            lookback_hours=1
        )
        
        filter_expr = query_result['metadata']['filter_expression']
        reasoning = query_result['metadata']['reasoning']
        signoz_query = query_result['query']
        
        print(f"   ✓ Query generated successfully")
        print(f"\n   Filter Expression:")
        print(f"   {filter_expr}")
        print(f"\n   Reasoning:")
        print(f"   {reasoning}\n")
        
        assert query_result is not None
        assert 'query' in query_result
        assert 'metadata' in query_result
        
    except Exception as e:
        pytest.fail(f"LLM query generation failed: {e}")
    
    # ============================================================================
    # STEP 3: Fetch Logs from SigNoz
    # ============================================================================
    print("STEP 3: Fetch Logs from SigNoz Cloud")
    print("-" * 80)
    
    try:
        raw_response = signoz_client.fetch_logs(
            query_payload=signoz_query,
            incident_id=incident_id
        )
        
        raw_response_size = len(json.dumps(raw_response))
        
        print(f"   ✓ Logs fetched from SigNoz")
        print(f"   Endpoint: {signoz_client.api_endpoint}")
        print(f"   Response size: {raw_response_size:,} bytes\n")
        
        assert raw_response is not None
        
    except Exception as e:
        pytest.fail(f"SigNoz fetch failed: {e}")
    
    # ============================================================================
    # STEP 4: Transform Logs
    # ============================================================================
    print("STEP 4: Transform Logs to Standard Format")
    print("-" * 80)
    
    try:
        transformed_logs = LogTransformer.transform_logs(raw_response)
        
        print(f"   ✓ Logs transformed successfully")
        print(f"   Total logs: {len(transformed_logs)}\n")
        
        if transformed_logs:
            print("   Sample Transformed Log:")
            sample = transformed_logs[0]
            print(f"   - Timestamp: {sample.get('timestamp')}")
            print(f"   - Service: {sample.get('service')}")
            print(f"   - Level: {sample.get('level')}")
            print(f"   - Status: {sample.get('status_code')}")
            print(f"   - Message: {sample.get('message', '')[:60]}...\n")
        
        assert isinstance(transformed_logs, list)
        
    except Exception as e:
        pytest.fail(f"Log transformation failed: {e}")
    
    # ============================================================================
    # STEP 5: Store Logs in S3
    # ============================================================================
    print("STEP 5: Store Logs in S3 (LocalStack/AWS)")
    print("-" * 80)
    
    try:
        s3_key = s3_storage.upload_logs(
            logs=transformed_logs,
            incident_id=incident_id,
            metadata={
                "source": "e2e_test",
                "filter_expression": filter_expr,
                "signoz_endpoint": signoz_client.api_endpoint
            },
            file_type="logs"
        )
        
        s3_uri = f"s3://{s3_storage.bucket_name}/{s3_key}"
        
        print(f"   ✓ Logs uploaded to S3")
        print(f"   Bucket: {s3_storage.bucket_name}")
        print(f"   Key: {s3_key}")
        print(f"   URI: {s3_uri}")
        print(f"   Backend: {'LocalStack' if s3_storage.is_local else 'AWS S3'}\n")
        
        assert s3_key is not None
        
    except Exception as e:
        pytest.fail(f"S3 upload failed: {e}")
    
    # ============================================================================
    # STEP 6: Verify Stored Data
    # ============================================================================
    print("STEP 6: Verify Stored Data")
    print("-" * 80)
    
    try:
        # Download from S3
        downloaded_data = s3_storage.download_logs(s3_key)
        
        # Verify integrity
        assert 'metadata' in downloaded_data
        assert 'logs' in downloaded_data
        assert len(downloaded_data['logs']) == len(transformed_logs)
        
        print(f"   ✓ Downloaded and verified {len(downloaded_data['logs'])} logs")
        print(f"   ✓ Data integrity confirmed\n")
        
        # List all files for incident
        incident_files = s3_storage.list_incident_files(incident_id)
        print(f"   Total files for incident: {len(incident_files)}")
        
    except Exception as e:
        pytest.fail(f"Verification failed: {e}")
    
    # ============================================================================
    # STEP 7: Generate Complete Report
    # ============================================================================
    print("\nSTEP 7: Generating Complete Test Report")
    print("-" * 80)
    
    report = {
        "test_suite": "end_to_end_integration",
        "test_name": "complete_incident_log_workflow",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "duration_seconds": None,  # Can add timing if needed
        
        # Complete Data Flow
        "data_flow": {
            
            # Step 1: Input
            "01_incident_payload": {
                "incident_id": incident_id,
                "service": test_payload['service']['name'],
                "title": test_payload['title'],
                "full_payload": test_payload
            },
            
            # Step 2: LLM Query Generation
            "02_llm_query_generation": {
                "filter_expression": filter_expr,
                "reasoning": reasoning,
                "key_attributes": query_result['metadata']['key_attributes'],
                "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
                "signoz_query": signoz_query
            },
            
            # Step 3: SigNoz Fetch
            "03_signoz_fetch": {
                "endpoint": signoz_client.api_endpoint,
                "api_version": "v5",
                "raw_response_size_bytes": raw_response_size,
                "fetch_successful": True
            },
            
            # Step 4: Log Transformation
            "04_log_transformation": {
                "logs_transformed": len(transformed_logs),
                "sample_log": transformed_logs[0] if transformed_logs else None,
                "transformed_logs_preview": transformed_logs[:5] if len(transformed_logs) >= 5 else transformed_logs
            },
            
            # Step 5: S3 Storage
            "05_s3_storage": {
                "s3_bucket": s3_storage.bucket_name,
                "s3_key": s3_key,
                "s3_uri": s3_uri,
                "storage_backend": "LocalStack" if s3_storage.is_local else "AWS S3",
                "upload_successful": True
            },
            
            # Step 6: Verification
            "06_verification": {
                "download_successful": True,
                "data_integrity_verified": True,
                "logs_verified": len(downloaded_data['logs']),
                "files_in_incident": len(incident_files)
            }
        },
        
        # Summary
        "summary": {
            "total_steps": 6,
            "steps_passed": 6,
            "steps_failed": 0,
            "logs_processed": len(transformed_logs),
            "storage_verified": True
        },
        
        # Component Info
        "components": {
            "llm": {
                "service": "AWS Bedrock",
                "model": "Claude 3.5 Sonnet"
            },
            "observability": {
                "service": "SigNoz Cloud",
                "endpoint": signoz_client.api_endpoint
            },
            "storage": {
                "service": "LocalStack" if s3_storage.is_local else "AWS S3",
                "bucket": s3_storage.bucket_name
            }
        }
    }
    
    filepath = save_test_result(reports_dir, "e2e_complete_workflow", report)
    print(f"   ✓ Complete report saved to: {filepath}\n")
    
    print("="*80)
    print("   END-TO-END TEST PASSED ✓")
    print("   All 6 steps completed successfully!")
    print("="*80 + "\n")
