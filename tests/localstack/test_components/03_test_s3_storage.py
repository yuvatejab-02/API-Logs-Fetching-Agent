#!/usr/bin/env python3
"""Test 03: S3 Storage - Verify logs are stored in LocalStack S3."""
import json
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
import boto3

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Simple logging (no config needed)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

LOCALSTACK_ENDPOINT = "http://localhost:14566"
S3_BUCKET = "incident-logs-test"


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
    """Test S3 storage."""
    print("\n" + "="*80)
    print("   TEST 03: S3 Storage (LocalStack)")
    print("="*80 + "\n")
    
    # Create S3 client
    print(" Step 1: Connecting to LocalStack S3...")
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=LOCALSTACK_ENDPOINT,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        print(f"    Connected to: {LOCALSTACK_ENDPOINT}\n")
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Test data
    test_logs = [
        {
            "timestamp": "2025-10-25T11:53:23.595078912Z",
            "service": "payments-service",
            "instance_id": "payments-service-e86baad2",
            "level": "ERROR",
            "request_id": "943603bf97be0f92a50e8a56f1a1a647",
            "company_id": "testing",
            "user_id": "user-7648",
            "method": "GET",
            "path": "/api/v1/products",
            "status_code": 503,
            "response_time_ms": 85,
            "message": "GET /api/v1/products - 503 (85ms)"
        },
        {
            "timestamp": "2025-10-25T11:53:23.595078912Z",
            "service": "payments-service",
            "instance_id": "payments-service-e86baad2",
            "level": "ERROR",
            "request_id": "943603bf97be0f92a50e8a56f1a1a647",
            "company_id": "testing",
            "user_id": "user-7648",
            "method": "GET",
            "path": "/api/v1/products",
            "status_code": 503,
            "response_time_ms": 85,
            "message": "GET /api/v1/products - 503 (85ms)"
        }
    ]
    
    incident_id = "INC_E2E_001"
    s3_key = f"incidents/{incident_id}/test/logs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    
    # Upload to S3
    print(" Step 2: Uploading test logs to S3...")
    try:
        upload_data = {
            "metadata": {
                "incident_id": incident_id,
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
                "log_count": len(test_logs),
                "test": True
            },
            "logs": test_logs
        }
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(upload_data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        print(f"    Uploaded to: s3://{S3_BUCKET}/{s3_key}\n")
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Verify upload
    print(" Step 3: Verifying upload (download and check)...")
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        downloaded_data = json.loads(response['Body'].read().decode('utf-8'))
        
        if len(downloaded_data['logs']) == len(test_logs):
            print(f"    Verified: {len(downloaded_data['logs'])} logs\n")
        else:
            print(f"    Mismatch: Expected {len(test_logs)}, got {len(downloaded_data['logs'])}\n")
            return False
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # List all files for incident
    print(" Step 4: Listing all files for incident...")
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"incidents/{incident_id}/"
        )
        
        files = []
        if 'Contents' in response:
            files = response['Contents']
            print(f"    Found {len(files)} file(s):")
            for obj in files:
                size_kb = obj['Size'] / 1024
                print(f"      - {obj['Key']} ({size_kb:.2f} KB)")
        else:
            print("     No files found")
        print()
    except Exception as e:
        print(f"    Failed: {e}\n")
        return False
    
    # Prepare data flow
    data_flow = {
        "test_name": "03_s3_storage",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "passed",
        "input": {
            "logs_count": len(test_logs),
            "incident_id": incident_id
        },
        "output": {
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "s3_endpoint": LOCALSTACK_ENDPOINT,
            "upload_verified": True,
            "files_in_incident": len(files) if files else 0
        },
        "storage_info": {
            "backend": "LocalStack S3",
            "region": "us-east-1"
        }
    }
    
    # Save result
    print(" Step 5: Saving test results...")
    filepath = save_test_result("03_s3_storage", data_flow)
    print(f"    Saved to: {filepath}\n")
    
    print("="*80)
    print("   TEST 03 PASSED")
    print("="*80 + "\n")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
