"""Check messages in the output queue."""
import json
import boto3

# Configuration
LOCALSTACK_ENDPOINT = "http://localhost:14566"
OUTPUT_QUEUE_URL = "http://localhost:14566/000000000000/incident-output-queue"
REGION = "us-east-1"

# Initialize SQS client
sqs = boto3.client(
    'sqs',
    region_name=REGION,
    endpoint_url=LOCALSTACK_ENDPOINT
)

# Receive messages
response = sqs.receive_message(
    QueueUrl=OUTPUT_QUEUE_URL,
    MaxNumberOfMessages=10,
    WaitTimeSeconds=1
)

messages = response.get('Messages', [])

if messages:
    print(f"\n✅ Found {len(messages)} completion message(s) in output queue:\n")
    print("="*80)
    
    for msg in messages:
        body = json.loads(msg['Body'])
        print(json.dumps(body, indent=2))
        print("="*80)
else:
    print("\n❌ No messages found in output queue")


