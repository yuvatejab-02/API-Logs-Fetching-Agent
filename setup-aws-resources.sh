#!/bin/bash
# Setup AWS Resources for API Logs Fetching Agent - Dev Environment
# Region: ap-southeast-1

set -e

REGION="ap-southeast-1"
ACCOUNT_ID="528104389666"

echo "=========================================="
echo "Creating SQS FIFO Queues for Dev"
echo "=========================================="

# Create Input Queue (FIFO)
echo "Creating input queue: api-fetcher-dev-input-queue.fifo"
aws sqs create-queue \
  --queue-name api-fetcher-dev-input-queue.fifo \
  --region $REGION \
  --attributes '{
    "FifoQueue": "true",
    "ContentBasedDeduplication": "false",
    "DeduplicationScope": "messageGroup",
    "FifoThroughputLimit": "perMessageGroupId",
    "MessageRetentionPeriod": "14400",
    "MaximumMessageSize": "10240",
    "VisibilityTimeout": "900",
    "ReceiveMessageWaitTimeSeconds": "15",
    "SqsManagedSseEnabled": "true"
  }'

INPUT_QUEUE_URL="https://sqs.$REGION.amazonaws.com/$ACCOUNT_ID/api-fetcher-dev-input-queue.fifo"
echo "✅ Input queue created: $INPUT_QUEUE_URL"

# Create Output Queue (FIFO)
echo ""
echo "Creating output queue: api-fetcher-dev-output-queue.fifo"
aws sqs create-queue \
  --queue-name api-fetcher-dev-output-queue.fifo \
  --region $REGION \
  --attributes '{
    "FifoQueue": "true",
    "ContentBasedDeduplication": "false",
    "DeduplicationScope": "messageGroup",
    "FifoThroughputLimit": "perMessageGroupId",
    "MessageRetentionPeriod": "14400",
    "MaximumMessageSize": "10240",
    "VisibilityTimeout": "900",
    "ReceiveMessageWaitTimeSeconds": "15",
    "SqsManagedSseEnabled": "true"
  }'

OUTPUT_QUEUE_URL="https://sqs.$REGION.amazonaws.com/$ACCOUNT_ID/api-fetcher-dev-output-queue.fifo"
echo "✅ Output queue created: $OUTPUT_QUEUE_URL"

echo ""
echo "=========================================="
echo "Queue URLs (save these for task definition):"
echo "=========================================="
echo "INPUT_QUEUE:  $INPUT_QUEUE_URL"
echo "OUTPUT_QUEUE: $OUTPUT_QUEUE_URL"
echo ""

echo "=========================================="
echo "Setting up AWS Systems Manager Parameters"
echo "=========================================="

# Note: You'll need to provide the actual secret values
echo ""
echo "⚠️  IMPORTANT: Run these commands manually with your actual secret values:"
echo ""
echo "# AWS Access Key ID"
echo "aws ssm put-parameter \\"
echo "  --name '/dev/api-logs-fetching-agent/aws-access-key-id' \\"
echo "  --value 'YOUR_ACCESS_KEY_ID' \\"
echo "  --type 'SecureString' \\"
echo "  --region $REGION \\"
echo "  --overwrite"
echo ""
echo "# AWS Secret Access Key"
echo "aws ssm put-parameter \\"
echo "  --name '/dev/api-logs-fetching-agent/aws-secret-access-key' \\"
echo "  --value 'YOUR_SECRET_ACCESS_KEY' \\"
echo "  --type 'SecureString' \\"
echo "  --region $REGION \\"
echo "  --overwrite"
echo ""
echo "# SigNoz API Key"
echo "aws ssm put-parameter \\"
echo "  --name '/dev/api-logs-fetching-agent/signoz-api-key' \\"
echo "  --value 'YOUR_SIGNOZ_API_KEY' \\"
echo "  --type 'SecureString' \\"
echo "  --region $REGION \\"
echo "  --overwrite"
echo ""

echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="

