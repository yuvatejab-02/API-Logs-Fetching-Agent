"""Helper script to send test messages to SQS for testing."""
import json
import boto3
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
LOCALSTACK_ENDPOINT = "http://localhost:14566"
INPUT_QUEUE_URL = "http://localhost:14566/000000000000/incident-input-queue"
REGION = "us-east-1"

# Sample incident payload in NEW format
SAMPLE_PAYLOAD = {
    "job_type": "incident-data-fetch",
    "data_sources": [
        {
            "source_type": "signoz",
            "connection_info": {
                "api_endpoint": os.getenv("SIGNOZ_API_ENDPOINT", "https://selected-starling.us.signoz.cloud")
            },
            "auth_config": {
                "auth_type": "api_key",
                "api_key": os.getenv("SIGNOZ_API_KEY", "YOUR_API_KEY_HERE")
            }
        }
    ],
    "incident": {
        "incident_id": "INC_test_001",
        "company_id": "testcompany",
        "title": "Spike in 5xx errors for payments service",
        "service": {
            "name": "payments-service"
        },
        "environment": "prod",
        "lookback_hours": 1
    }
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
        # Prepare send parameters
        send_params = {
            'QueueUrl': queue_url,
            'MessageBody': json.dumps(payload)
        }
        
        # Add FIFO-specific parameters if queue is FIFO
        if queue_url.endswith('.fifo'):
            incident_id = payload.get('incident', {}).get('incident_id', 'default')
            send_params['MessageGroupId'] = incident_id
            send_params['MessageDeduplicationId'] = f"{incident_id}_{int(datetime.now().timestamp() * 1000)}"
            print(f"   FIFO Queue detected - using MessageGroupId: {incident_id}")
        
        # Send message
        response = sqs.send_message(**send_params)
        
        print(f"✅ Message sent successfully!")
        print(f"   Message ID: {response['MessageId']}")
        print(f"   Incident ID: {payload['incident']['incident_id']}")
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
        "--company-id",
        type=str,
        help="Company ID (default: testcompany)"
    )
    parser.add_argument(
        "--service",
        type=str,
        help="Service name (default: payments-service)"
    )
    parser.add_argument(
        "--title",
        type=str,
        help="Incident title (default: Spike in 5xx errors for payments service)"
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=1,
        help="Lookback hours (default: 1)"
    )
    parser.add_argument(
        "--environment",
        type=str,
        choices=["prod", "staging", "dev"],
        default="prod",
        help="Environment (default: prod)"
    )
    parser.add_argument(
        "--signoz-endpoint",
        type=str,
        help="SigNoz API endpoint (default: from .env or sample)"
    )
    parser.add_argument(
        "--signoz-api-key",
        type=str,
        help="SigNoz API key (default: from .env or sample)"
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Send to production SQS (not LocalStack)"
    )
    
    args = parser.parse_args()
    
    # Deep copy payload
    payload = json.loads(json.dumps(SAMPLE_PAYLOAD))
    
    # Customize incident section
    if args.incident_id:
        payload["incident"]["incident_id"] = args.incident_id
    
    if args.company_id:
        payload["incident"]["company_id"] = args.company_id
    
    if args.service:
        payload["incident"]["service"]["name"] = args.service
    
    if args.title:
        payload["incident"]["title"] = args.title
    
    if args.lookback_hours:
        payload["incident"]["lookback_hours"] = args.lookback_hours
    
    if args.environment:
        payload["incident"]["environment"] = args.environment
    
    # Customize data source section
    if args.signoz_endpoint:
        payload["data_sources"][0]["connection_info"]["api_endpoint"] = args.signoz_endpoint
    
    if args.signoz_api_key:
        payload["data_sources"][0]["auth_config"]["api_key"] = args.signoz_api_key
    
    # Validate API key is set
    api_key = payload["data_sources"][0]["auth_config"]["api_key"]
    if api_key == "YOUR_API_KEY_HERE":
        print("\n❌ ERROR: SigNoz API key not set!")
        print("   Please set SIGNOZ_API_KEY in .env file or use --signoz-api-key argument")
        sys.exit(1)
    
    # Display payload (sanitized)
    display_payload = json.loads(json.dumps(payload))
    display_payload["data_sources"][0]["auth_config"]["api_key"] = "***REDACTED***"
    
    print("\n" + "="*80)
    print("  📤 SENDING TEST MESSAGE TO SQS")
    print("="*80)
    print(f"\nPayload (API key redacted):")
    print(json.dumps(display_payload, indent=2))
    print("\n" + "="*80 + "\n")
    
    # Send message
    send_message(payload, use_localstack=not args.production)


if __name__ == "__main__":
    main()
