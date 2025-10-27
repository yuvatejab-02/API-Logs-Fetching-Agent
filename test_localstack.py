#!/usr/bin/env python3
"""Test LocalStack S3 connectivity and operations."""

import boto3
import json
from pathlib import Path
from datetime import datetime

# Configure for LocalStack
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)


def print_section(title: str):
    """Print formatted section header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def list_buckets():
    """List all S3 buckets."""
    print_section("📦 S3 BUCKETS")
    try:
        response = s3.list_buckets()
        
        if response['Buckets']:
            for bucket in response['Buckets']:
                created = bucket['CreationDate'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"  ✅ {bucket['Name']:30} (created: {created})")
        else:
            print("  ⚠️  No buckets found. Run setup_localstack.py first.")
        
        return [b['Name'] for b in response['Buckets']]
        
    except Exception as e:
        print(f"  ❌ Error listing buckets: {e}")
        return []


def list_incident_logs():
    """List files in incident-logs bucket."""
    print_section("📁 INCIDENT LOGS - Root Level")
    
    try:
        response = s3.list_objects_v2(
            Bucket='incident-logs',
            Delimiter='/'
        )
        
        if 'CommonPrefixes' in response:
            print("  Folders:")
            for prefix in response['CommonPrefixes']:
                print(f"    📂 {prefix['Prefix']}")
        
        if 'Contents' in response:
            print("\n  Files:")
            for obj in response['Contents']:
                size_kb = obj['Size'] / 1024
                modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"    📄 {obj['Key']:50} ({size_kb:.2f} KB, {modified})")
        
        if 'Contents' not in response and 'CommonPrefixes' not in response:
            print("  ⚠️  Bucket is empty")
            
    except s3.exceptions.NoSuchBucket:
        print("  ❌ Bucket 'incident-logs' does not exist")
    except Exception as e:
        print(f"  ❌ Error: {e}")


def list_specific_incident(incident_id: str = "INC_polling_test"):
    """List all files for a specific incident."""
    print_section(f"🔍 INCIDENT FILES - {incident_id}")
    
    prefix = f"incidents/{incident_id}/"
    
    try:
        response = s3.list_objects_v2(
            Bucket='incident-logs',
            Prefix=prefix
        )
        
        if 'Contents' in response:
            print(f"  Found {len(response['Contents'])} files:\n")
            
            for obj in response['Contents']:
                key = obj['Key']
                size_kb = obj['Size'] / 1024
                modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                
                # Add visual hierarchy
                relative_path = key.replace(prefix, '')
                indent = '    ' * (relative_path.count('/'))
                filename = relative_path.split('/')[-1] if '/' in relative_path else relative_path
                
                print(f"  {indent}📄 {filename:40} ({size_kb:.2f} KB, {modified})")
        else:
            print(f"  ⚠️  No files found for incident: {incident_id}")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")


def download_and_verify_file(
    incident_id: str = "INC_polling_test",
    s3_path: str = "final_aggregated/20251025_140000.json",
    local_path: str = "./verify.json"
):
    """Download a file from S3 and display its contents."""
    print_section("📥 DOWNLOAD & VERIFY FILE")
    
    s3_key = f"incidents/{incident_id}/{s3_path}"
    
    try:
        print(f"  Downloading: s3://incident-logs/{s3_key}")
        print(f"  To: {local_path}\n")
        
        # Download file
        s3.download_file('incident-logs', s3_key, local_path)
        
        # Get file size
        file_size = Path(local_path).stat().st_size
        print(f"  ✅ Downloaded successfully ({file_size} bytes)")
        
        # Read and display contents
        with open(local_path, 'r') as f:
            data = json.load(f)
        
        print(f"\n  📄 File Contents Preview:")
        print(f"  {'-'*76}")
        print(json.dumps(data, indent=2)[:500] + "..." if len(json.dumps(data)) > 500 else json.dumps(data, indent=2))
        print(f"  {'-'*76}")
        
        return True
        
    except s3.exceptions.NoSuchKey:
        print(f"  ❌ File not found: {s3_key}")
        return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False


def create_test_file():
    """Create a test file in S3."""
    print_section("📤 UPLOAD TEST FILE")
    
    test_data = {
        "incident_id": "TEST_001",
        "timestamp": datetime.now().isoformat(),
        "message": "Test file created by test_localstack.py",
        "status": "success"
    }
    
    try:
        s3.put_object(
            Bucket='incident-logs',
            Key='test/test_file.json',
            Body=json.dumps(test_data, indent=2),
            ContentType='application/json'
        )
        print("  ✅ Test file uploaded: test/test_file.json")
        return True
    except Exception as e:
        print(f"  ❌ Error uploading: {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("  🧪 LOCALSTACK S3 TEST SUITE")
    print("="*80)
    print(f"  Endpoint: http://localhost:4566")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Test 1: List buckets
        buckets = list_buckets()
        
        if not buckets:
            print("\n⚠️  No buckets found. Please run setup_localstack.py first.")
            return
        
        # Test 2: Check if incident-logs bucket exists
        if 'incident-logs' not in buckets:
            print("\n⚠️  'incident-logs' bucket not found. Run setup_localstack.py.")
            return
        
        # Test 3: List root level of incident-logs
        list_incident_logs()
        
        # Test 4: Create and upload a test file
        create_test_file()
        
        # Test 5: List specific incident (if exists)
        list_specific_incident("INC_polling_test")
        
        # Test 6: Try to download a file (if exists)
        download_and_verify_file(
            incident_id="INC_polling_test",
            s3_path="final_aggregated/20251025_140000.json",
            local_path="./verify.json"
        )
        
        # Final summary
        print_section("✅ TEST SUMMARY")
        print("  All LocalStack S3 operations completed!")
        print("  Use 'aws --endpoint-url=http://localhost:4566 s3 ls' for AWS CLI")
        print("\n")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")


if __name__ == "__main__":
    main()
