#!/bin/bash

set -e

echo "============================================"
echo "LocalStack Initialization Script Starting"
echo "============================================"

# Wait for LocalStack to be fully ready
echo "Waiting for LocalStack to be ready..."
sleep 3

# Test if awslocal is available
if ! command -v awslocal &> /dev/null; then
    echo "ERROR: awslocal command not found!"
    exit 1
fi

echo ""
echo "Creating SQS queues..."
echo "--------------------------------------------"

# Input queue for incident payloads
echo "Creating incident-input-queue..."
awslocal sqs create-queue \
    --queue-name incident-input-queue \
    --region us-east-1 \
    --attributes VisibilityTimeout=300 || echo "Queue may already exist"

# Output queue for job completion notifications
echo "Creating incident-output-queue..."
awslocal sqs create-queue \
    --queue-name incident-output-queue \
    --region us-east-1 \
    --attributes VisibilityTimeout=300 || echo "Queue may already exist"

# Dead letter queue for failed messages
echo "Creating incident-dlq..."
awslocal sqs create-queue \
    --queue-name incident-dlq \
    --region us-east-1 \
    --attributes VisibilityTimeout=300 || echo "Queue may already exist"

echo ""
echo "Verifying queues..."
echo "--------------------------------------------"
awslocal sqs list-queues --region us-east-1

# Get and display queue URLs
echo ""
echo "Queue URLs:"
echo "--------------------------------------------"
INPUT_QUEUE=$(awslocal sqs get-queue-url --queue-name incident-input-queue --region us-east-1 --output text 2>/dev/null || echo "ERROR: Could not get input queue URL")
OUTPUT_QUEUE=$(awslocal sqs get-queue-url --queue-name incident-output-queue --region us-east-1 --output text 2>/dev/null || echo "ERROR: Could not get output queue URL")
DLQ=$(awslocal sqs get-queue-url --queue-name incident-dlq --region us-east-1 --output text 2>/dev/null || echo "ERROR: Could not get DLQ URL")

echo "Input Queue:  $INPUT_QUEUE"
echo "Output Queue: $OUTPUT_QUEUE"
echo "DLQ:          $DLQ"

# Create S3 bucket for local testing
echo ""
echo "Creating S3 bucket..."
echo "--------------------------------------------"
awslocal s3 mb s3://incident-logs-test --region us-east-1 2>/dev/null || echo "Bucket 'incident-logs-test' already exists"
awslocal s3 ls

echo ""
echo "============================================"
echo "✅ LocalStack initialization complete!"
echo "============================================"


