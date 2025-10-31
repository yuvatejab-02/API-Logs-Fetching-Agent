# Setup AWS Resources for API Logs Fetching Agent - Dev Environment
# Region: ap-southeast-1

$ErrorActionPreference = "Stop"

$REGION = "ap-southeast-1"
$ACCOUNT_ID = "528104389666"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Creating SQS FIFO Queues for Dev" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Create Input Queue (FIFO)
Write-Host "`nCreating input queue: api-fetcher-dev-input-queue.fifo" -ForegroundColor Yellow
python -m awscli sqs create-queue `
  --queue-name api-fetcher-dev-input-queue.fifo `
  --region $REGION `
  --attributes '{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"DeduplicationScope\":\"messageGroup\",\"FifoThroughputLimit\":\"perMessageGroupId\",\"MessageRetentionPeriod\":\"14400\",\"MaximumMessageSize\":\"10240\",\"VisibilityTimeout\":\"900\",\"ReceiveMessageWaitTimeSeconds\":\"15\",\"SqsManagedSseEnabled\":\"true\"}'

$INPUT_QUEUE_URL = "https://sqs.$REGION.amazonaws.com/$ACCOUNT_ID/api-fetcher-dev-input-queue.fifo"
Write-Host "✅ Input queue created: $INPUT_QUEUE_URL" -ForegroundColor Green

# Create Output Queue (FIFO)
Write-Host "`nCreating output queue: api-fetcher-dev-output-queue.fifo" -ForegroundColor Yellow
python -m awscli sqs create-queue `
  --queue-name api-fetcher-dev-output-queue.fifo `
  --region $REGION `
  --attributes '{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"DeduplicationScope\":\"messageGroup\",\"FifoThroughputLimit\":\"perMessageGroupId\",\"MessageRetentionPeriod\":\"14400\",\"MaximumMessageSize\":\"10240\",\"VisibilityTimeout\":\"900\",\"ReceiveMessageWaitTimeSeconds\":\"15\",\"SqsManagedSseEnabled\":\"true\"}'

$OUTPUT_QUEUE_URL = "https://sqs.$REGION.amazonaws.com/$ACCOUNT_ID/api-fetcher-dev-output-queue.fifo"
Write-Host "✅ Output queue created: $OUTPUT_QUEUE_URL" -ForegroundColor Green

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host "Queue URLs (save these for task definition):" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "INPUT_QUEUE:  $INPUT_QUEUE_URL" -ForegroundColor White
Write-Host "OUTPUT_QUEUE: $OUTPUT_QUEUE_URL" -ForegroundColor White

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host "Setting up AWS Systems Manager Parameters" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n⚠️  IMPORTANT: The secrets are already configured in SSM:" -ForegroundColor Yellow
Write-Host "   - /dev/api-logs-fetching-agent/aws-access-key-id" -ForegroundColor White
Write-Host "   - /dev/api-logs-fetching-agent/aws-secret-access-key" -ForegroundColor White
Write-Host "   - /dev/api-logs-fetching-agent/signoz-api-key" -ForegroundColor White
Write-Host "`nThese are referenced in your task definition and will be injected at runtime." -ForegroundColor White

Write-Host "`n==========================================" -ForegroundColor Green
Write-Host "✅ Setup Complete!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green

