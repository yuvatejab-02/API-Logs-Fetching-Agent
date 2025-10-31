#!/bin/bash

# Wait for LocalStack to be ready
echo "Waiting for LocalStack to be ready..."
sleep 2

# Create SQS queues for local testing (standard queues)
echo "Creating SQS queues..."

# Input queue for incident payloads
awslocal sqs create-queue \
    --queue-name incident-input-queue \
    --region us-east-1

# Output queue for job completion notifications
awslocal sqs create-queue \
    --queue-name incident-output-queue \
    --region us-east-1

# Dead letter queue for failed messages
awslocal sqs create-queue \
    --queue-name incident-dlq \
    --region us-east-1

echo "SQS queues created successfully!"

# List queues to verify
echo "Available queues:"
awslocal sqs list-queues --region us-east-1


