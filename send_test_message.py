"""Helper script to send test messages to SQS for testing."""
import json
import boto3
import sys
from datetime import datetime

# Configuration
LOCALSTACK_ENDPOINT = "http://localhost:14566"
INPUT_QUEUE_URL = "http://localhost:14566/000000000000/incident-input-queue"
REGION = "us-east-1"

# Sample incident payload
SAMPLE_PAYLOAD = {
    "compslug": "testcompany",
    "Session_id": "7b3f2f30-0c4b-4c42-9f8f-7e3d2b8e2a61",
    "incident_id": "INC_test_001",
    "title": "Spike in 5xx errors for payments service",
    "service": {
        "id": "svc_payments_123",
        "name": "payments-service"
    },
    "environment": "prod",
    "tenant": "default",
    "lookback_hours": 1,
    "severity": "high",
    "description": "Payment service experiencing elevated 5xx error rates"
}


def send_message(payload: dict, use_localstack: bool = True):
    """Send a message to SQS.
    
    Args:
        payload: Incident payload to send
        use_localstack: Whether to use LocalStack (default: True)
    """
    # Initialize SQS client
    if use_localstack:
        sqs = boto3.client(
            'sqs',
            region_name=REGION,
            endpoint_url=LOCALSTACK_ENDPOINT
        )
        queue_url = INPUT_QUEUE_URL
    else:
        sqs = boto3.client('sqs', region_name=REGION)
        queue_url = input("Enter SQS queue URL: ")
    
    try:
        # Send message
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(payload)
        )
        
        print(f"✅ Message sent successfully!")
        print(f"   Message ID: {response['MessageId']}")
        print(f"   Incident ID: {payload['incident_id']}")
        print(f"   Queue: {queue_url}")
        
    except Exception as e:
        print(f"❌ Failed to send message: {str(e)}")
        sys.exit(1)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Send test message to SQS")
    parser.add_argument(
        "--incident-id",
        type=str,
        help="Custom incident ID (default: INC_test_001)"
    )
    parser.add_argument(
        "--service",
        type=str,
        help="Service name (default: payments-service)"
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=1,
        help="Lookback hours (default: 1)"
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Send to production SQS (not LocalStack)"
    )
    
    args = parser.parse_args()
    
    # Customize payload if needed
    payload = SAMPLE_PAYLOAD.copy()
    
    if args.incident_id:
        payload["incident_id"] = args.incident_id
    
    if args.service:
        payload["service"]["name"] = args.service
    
    if args.lookback_hours:
        payload["lookback_hours"] = args.lookback_hours
    
    # Display payload
    print("\n" + "="*80)
    print("  📤 SENDING TEST MESSAGE TO SQS")
    print("="*80)
    print(f"\nPayload:")
    print(json.dumps(payload, indent=2))
    print("\n" + "="*80 + "\n")
    
    # Send message
    send_message(payload, use_localstack=not args.production)


if __name__ == "__main__":
    main()


