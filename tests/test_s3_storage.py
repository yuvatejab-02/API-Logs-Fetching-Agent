"""Test S3 storage operations with LocalStack."""
import pytest
import json
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

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


@pytest.fixture(scope="session")
def s3_storage():
    """Initialize S3Storage (will use LocalStack if IS_LOCAL_ENVIRONMENT=true)."""
    return S3Storage()


@pytest.fixture
def test_logs():
    """Generate test log data (transformed format)."""
    return [
        {
            "timestamp": "2025-10-28T10:00:00.123Z",
            "service": "payments-service",
            "instance_id": "payments-service-xyz123",
            "level": "ERROR",
            "request_id": "req-abc-123",
            "company_id": "testing",
            "user_id": "user-1234",
            "method": "GET",
            "path": "/api/v1/products",
            "status_code": 503,
            "response_time_ms": 85,
            "message": "GET /api/v1/products - 503 (85ms)"
        },
        {
            "timestamp": "2025-10-28T10:00:05.456Z",
            "service": "payments-service",
            "instance_id": "payments-service-xyz123",
            "level": "ERROR",
            "request_id": "req-def-456",
            "company_id": "testing",
            "user_id": "user-5678",
            "method": "POST",
            "path": "/api/v1/orders",
            "status_code": 500,
            "response_time_ms": 120,
            "error_message": "Database connection timeout",
            "message": "POST /api/v1/orders - 500 (120ms)"
        },
        {
            "timestamp": "2025-10-28T10:00:10.789Z",
            "service": "payments-service",
            "instance_id": "payments-service-xyz123",
            "level": "ERROR",
            "request_id": "req-ghi-789",
            "company_id": "testing",
            "user_id": "user-9012",
            "method": "DELETE",
            "path": "/api/v1/inventory",
            "status_code": 504,
            "response_time_ms": 95,
            "message": "DELETE /api/v1/inventory - 504 (95ms)"
        }
    ]


def save_test_result(reports_dir, test_name, data):
    """Save test result to reports directory."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{test_name}_{timestamp}.json"
    filepath = reports_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return str(filepath)


def test_s3_bucket_verification(s3_storage):
    """Test that S3 bucket exists and is accessible."""
    
    print("\n" + "="*80)
    print("  TEST: S3 Bucket Verification")
    print("="*80 + "\n")
    
    print("Step 1: Verifying S3 bucket...")
    print(f"   Bucket: {s3_storage.bucket_name}")
    print(f"   Backend: {'LocalStack' if s3_storage.is_local else 'AWS S3'}\n")
    
    # Bucket was verified during initialization
    assert s3_storage.bucket_name is not None
    assert s3_storage.s3_client is not None
    
    print("   ✓ S3 bucket verified and accessible\n")
    
    print("="*80)
    print("   TEST PASSED ✓")
    print("="*80 + "\n")


def test_s3_upload_download_verify(s3_storage, test_logs, reports_dir):
    """Test complete S3 flow: Upload → List → Download → Verify."""
    
    print("\n" + "="*80)
    print("  TEST: S3 Upload, Download & Verify")
    print("="*80 + "\n")
    
    incident_id = "INC_test_001"
    
    # Step 1: Display input
    print("Step 1: Input Data")
    print(f"   Incident ID: {incident_id}")
    print(f"   Logs to upload: {len(test_logs)}")
    print(f"   Backend: {'LocalStack' if s3_storage.is_local else 'AWS S3'}\n")
    
    # Step 2: Upload logs to S3
    print("Step 2: Uploading logs to S3...")
    try:
        s3_key = s3_storage.upload_logs(
            logs=test_logs,
            incident_id=incident_id,
            metadata={
                "source": "pytest",
                "test_run": True,
                "description": "Test upload for S3 storage validation"
            },
            file_type="logs"
        )
        
        print(f"   ✓ Logs uploaded successfully")
        print(f"   S3 Bucket: {s3_storage.bucket_name}")
        print(f"   S3 Key: {s3_key}")
        print(f"   S3 URI: s3://{s3_storage.bucket_name}/{s3_key}\n")
        
    except Exception as e:
        pytest.fail(f"Upload failed: {e}")
    
    # Step 3: List files for incident
    print("Step 3: Listing all files for incident...")
    try:
        files = s3_storage.list_incident_files(incident_id)
        
        print(f"   ✓ Found {len(files)} file(s)\n")
        
        for file_info in files:
            size_kb = file_info['size'] / 1024
            print(f"   - {file_info['key']}")
            print(f"     Size: {size_kb:.2f} KB")
            print(f"     Modified: {file_info['last_modified']}\n")
            
    except Exception as e:
        pytest.fail(f"List failed: {e}")
    
    # Step 4: Download and verify
    print("Step 4: Downloading and verifying logs...")
    try:
        downloaded_data = s3_storage.download_logs(s3_key)
        
        # Verify structure
        assert 'metadata' in downloaded_data, "Should have metadata"
        assert 'logs' in downloaded_data, "Should have logs"
        
        metadata = downloaded_data['metadata']
        logs = downloaded_data['logs']
        
        # Verify content
        assert metadata['incident_id'] == incident_id, "Incident ID should match"
        assert metadata['log_count'] == len(test_logs), "Log count should match"
        assert len(logs) == len(test_logs), "Downloaded logs count should match"
        
        print(f"   ✓ Downloaded {len(logs)} logs")
        print(f"   ✓ Verified all data integrity\n")
        
    except Exception as e:
        pytest.fail(f"Download/verify failed: {e}")
    
    # Step 5: Display sample
    print("Step 5: Sample Log from S3")
    print("-" * 80)
    if logs:
        sample = logs[0]
        print(f"   Timestamp: {sample.get('timestamp')}")
        print(f"   Service: {sample.get('service')}")
        print(f"   Level: {sample.get('level')}")
        print(f"   Status Code: {sample.get('status_code')}")
        print(f"   Message: {sample.get('message')}\n")
    
    # Step 6: Save report
    print("Step 6: Saving test results...")
    
    report = {
        "test_name": "03_s3_storage",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        
        # Input
        "input": {
            "incident_id": incident_id,
            "logs_count": len(test_logs),
            "file_type": "logs",
            "sample_log": test_logs[0] if test_logs else None
        },
        
        # S3 Operations
        "s3_operations": {
            "upload": {
                "s3_bucket": s3_storage.bucket_name,
                "s3_key": s3_key,
                "s3_uri": f"s3://{s3_storage.bucket_name}/{s3_key}",
                "logs_uploaded": len(test_logs)
            },
            "list": {
                "files_found": len(files),
                "file_details": files
            },
            "download": {
                "logs_downloaded": len(logs),
                "verified": True
            }
        },
        
        # Output - Downloaded Data
        "output": {
            "metadata": metadata,
            "logs": logs[:5] if len(logs) >= 5 else logs,  # First 5 logs
            "total_logs": len(logs)
        },
        
        # Storage Info
        "storage_info": {
            "backend": "LocalStack" if s3_storage.is_local else "AWS S3",
            "bucket": s3_storage.bucket_name,
            "is_local_environment": s3_storage.is_local
        }
    }
    
    filepath = save_test_result(reports_dir, "03_s3_storage", report)
    print(f"   ✓ Saved to: {filepath}\n")
    
    print("="*80)
    print("   TEST PASSED ✓")
    print("="*80 + "\n")


def test_s3_multiple_file_types(s3_storage, test_logs):
    """Test uploading different file types for same incident."""
    
    incident_id = "INC_test_002"
    
    # Upload logs
    logs_key = s3_storage.upload_logs(
        logs=test_logs,
        incident_id=incident_id,
        file_type="logs"
    )
    
    # Upload final aggregated
    final_data = {
        "summary": "Test aggregation",
        "error_count": len(test_logs),
        "primary_errors": ["503", "500", "504"]
    }
    
    final_key = s3_storage.upload_logs(
        logs=[final_data],  # Wrap in list
        incident_id=incident_id,
        file_type="final_aggregated"
    )
    
    # List all files
    files = s3_storage.list_incident_files(incident_id)
    
    # Verify we have both file types
    assert len(files) >= 2, "Should have at least 2 files"
    
    file_keys = [f['key'] for f in files]
    assert any('logs' in key for key in file_keys), "Should have logs file"
    assert any('final_aggregated' in key for key in file_keys), "Should have final_aggregated file"
    
    logger.info("multiple_file_types_verified", incident_id=incident_id, file_count=len(files))


def test_s3_empty_logs_handling(s3_storage):
    """Test handling of empty logs list."""
    
    incident_id = "INC_test_empty"
    empty_logs = []
    
    # Upload empty logs
    s3_key = s3_storage.upload_logs(
        logs=empty_logs,
        incident_id=incident_id,
        file_type="logs"
    )
    
    # Download and verify
    data = s3_storage.download_logs(s3_key)
    
    assert data['metadata']['log_count'] == 0, "Log count should be 0"
    assert len(data['logs']) == 0, "Logs should be empty"
    
    logger.info("empty_logs_handled")


def test_s3_list_nonexistent_incident(s3_storage):
    """Test listing files for non-existent incident."""
    
    files = s3_storage.list_incident_files("INC_nonexistent_999")
    
    assert files == [], "Should return empty list for non-existent incident"
    
    logger.info("nonexistent_incident_handled")
