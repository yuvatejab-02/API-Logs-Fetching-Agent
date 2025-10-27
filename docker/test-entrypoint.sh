#!/bin/bash
# Test entrypoint script for Docker

set -e

echo "================================"
echo "🐳 DOCKER E2E TEST STARTING"
echo "================================"

# Wait for LocalStack
echo "⏳ Waiting for LocalStack..."
until curl -s http://localstack:4566/_localstack/health | grep -q "running"; do
    echo "   LocalStack not ready yet..."
    sleep 2
done
echo "✅ LocalStack is ready!"

# Setup S3 bucket
echo "📦 Setting up S3 bucket..."
python -c "
import boto3
s3 = boto3.client(
    's3',
    endpoint_url='http://localstack:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)
try:
    s3.create_bucket(Bucket='incident-logs-test')
    print('✅ Bucket created')
except Exception as e:
    print(f'ℹ️  Bucket exists: {e}')
"

# Run the incident analyzer
echo "🚀 Starting incident analysis..."
python -m src.main "$@"

echo "================================"
echo "✅ DOCKER E2E TEST COMPLETE"
echo "================================"
