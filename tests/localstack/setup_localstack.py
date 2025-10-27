#!/usr/bin/env python3
"""Setup LocalStack resources for testing."""
import boto3
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

LOCALSTACK_ENDPOINT = "http://localhost:14566"
AWS_REGION = "us-east-1"
S3_BUCKET = "incident-logs-test"


def create_aws_clients():
    """Create AWS clients for LocalStack."""
    return boto3.client(
        's3',
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name=AWS_REGION
    )


def setup_s3_bucket(s3_client, bucket_name):
    """Create S3 bucket."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"ℹ️  Bucket already exists: {bucket_name}")
        return True
    except:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Created bucket: {bucket_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to create bucket: {e}")
            return False


def main():
    """Main setup function."""
    print("\n" + "="*80)
    print("  🚀 LOCALSTACK SETUP - INCIDENT LOG ANALYZER")
    print("="*80 + "\n")
    
    print(f"Endpoint: {LOCALSTACK_ENDPOINT}")
    print(f"Region: {AWS_REGION}")
    print(f"Bucket: {S3_BUCKET}\n")
    
    # Create S3 client
    s3_client = create_aws_clients()
    
    # Setup bucket
    if not setup_s3_bucket(s3_client, S3_BUCKET):
        return False
    
    print("\n" + "="*80)
    print("  ✅ SETUP COMPLETE")
    print("="*80 + "\n")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
