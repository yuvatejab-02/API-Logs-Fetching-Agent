"""SQS client for polling and sending messages."""
import json
import time
from typing import Dict, Any, Optional, Callable
import boto3
from botocore.exceptions import ClientError

from ..utils.logger import get_logger
from ..utils.sqs_schema import validate_sqs_message, create_job_completion_message

logger = get_logger(__name__)


class SQSClient:
    """Client for interacting with AWS SQS."""
    
    def __init__(
        self,
        input_queue_url: str,
        output_queue_url: str,
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None
    ):
        """Initialize SQS client.
        
        Args:
            input_queue_url: URL of input queue for incident payloads
            output_queue_url: URL of output queue for job completion
            region: AWS region
            endpoint_url: Optional endpoint URL (for LocalStack)
        """
        self.input_queue_url = input_queue_url
        self.output_queue_url = output_queue_url
        self.region = region
        
        # Determine if queue is FIFO
        self.is_input_fifo = input_queue_url.endswith('.fifo')
        self.is_output_fifo = output_queue_url.endswith('.fifo')
        
        # Initialize SQS client
        self.sqs = boto3.client(
            'sqs',
            region_name=region,
            endpoint_url=endpoint_url
        )
        
        logger.info(
            "sqs_client_initialized",
            input_queue=input_queue_url,
            output_queue=output_queue_url,
            input_is_fifo=self.is_input_fifo,
            output_is_fifo=self.is_output_fifo,
            endpoint=endpoint_url or "AWS"
        )
    
    def poll_messages(
        self,
        max_messages: int = 1,
        wait_time_seconds: int = 20,
        visibility_timeout: int = 300
    ) -> list:
        """Poll messages from input queue.
        
        Args:
            max_messages: Maximum number of messages to retrieve (1-10)
            wait_time_seconds: Long polling wait time
            visibility_timeout: Message visibility timeout in seconds
            
        Returns:
            List of messages
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.input_queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds,
                VisibilityTimeout=visibility_timeout,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            
            logger.info(
                "sqs_messages_polled",
                message_count=len(messages),
                queue=self.input_queue_url
            )
            
            return messages
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            
            # Check if it's a NonExistentQueue error
            if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                logger.warning(
                    "sqs_queue_not_found",
                    error=str(e),
                    queue=self.input_queue_url,
                    message="Queue does not exist yet. It may still be initializing."
                )
            else:
                logger.error(
                    "sqs_poll_failed",
                    error=str(e),
                    error_code=error_code,
                    queue=self.input_queue_url
                )
            raise
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process and validate a single SQS message.
        
        Args:
            message: Raw SQS message
            
        Returns:
            Validated incident payload or None if validation fails
        """
        try:
            # Extract message body
            body = json.loads(message['Body'])
            
            # Validate payload
            validated_payload = validate_sqs_message(body)
            
            # Add SQS metadata
            validated_payload['_sqs_metadata'] = {
                'message_id': message['MessageId'],
                'receipt_handle': message['ReceiptHandle'],
                'approximate_receive_count': message.get('Attributes', {}).get('ApproximateReceiveCount', '1')
            }
            
            return validated_payload
            
        except json.JSONDecodeError as e:
            logger.error(
                "sqs_message_invalid_json",
                error=str(e),
                message_id=message.get('MessageId')
            )
            return None
            
        except Exception as e:
            logger.error(
                "sqs_message_processing_failed",
                error=str(e),
                message_id=message.get('MessageId')
            )
            return None
    
    def delete_message(self, receipt_handle: str) -> bool:
        """Delete message from input queue after successful processing.
        
        Args:
            receipt_handle: Receipt handle from the message
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.sqs.delete_message(
                QueueUrl=self.input_queue_url,
                ReceiptHandle=receipt_handle
            )
            
            logger.info(
                "sqs_message_deleted",
                queue=self.input_queue_url
            )
            
            return True
            
        except ClientError as e:
            logger.error(
                "sqs_message_delete_failed",
                error=str(e),
                queue=self.input_queue_url
            )
            return False
    
    def send_completion_message(
        self,
        incident_id: str,
        company_id: str,
        service: str,
        environment: str,
        s3_urls: Dict[str, str]
    ) -> bool:
        """Send job completion message to output queue in new format.
        
        Args:
            incident_id: Incident identifier
            company_id: Company/tenant identifier
            service: Service name
            environment: Environment (prod, dev, staging)
            s3_urls: Dictionary of S3 URLs for each signal
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create completion message in new format
            completion_msg = create_job_completion_message(
                incident_id=incident_id,
                company_id=company_id,
                service=service,
                environment=environment,
                s3_urls=s3_urls
            )
            
            # Prepare send parameters
            send_params = {
                'QueueUrl': self.output_queue_url,
                'MessageBody': json.dumps(completion_msg)
            }
            
            # Add FIFO-specific parameters if needed
            if self.is_output_fifo:
                send_params['MessageGroupId'] = incident_id
                send_params['MessageDeduplicationId'] = f"{incident_id}_{int(time.time() * 1000)}"
            
            # Send message
            response = self.sqs.send_message(**send_params)
            
            logger.info(
                "sqs_completion_message_sent",
                incident_id=incident_id,
                company_id=company_id,
                message_id=response['MessageId'],
                queue=self.output_queue_url
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "sqs_completion_message_failed",
                incident_id=incident_id,
                error=str(e),
                queue=self.output_queue_url
            )
            return False
    
    def start_polling(
        self,
        message_handler: Callable[[Dict[str, Any]], bool],
        poll_interval: int = 20,
        max_empty_polls: int = None
    ):
        """Start continuous polling loop.
        
        Args:
            message_handler: Function to handle each message (returns True if successful)
            poll_interval: Seconds to wait between polls
            max_empty_polls: Maximum consecutive empty polls before stopping (None = infinite)
        """
        logger.info("starting_sqs_polling_loop", queue=self.input_queue_url, continuous=max_empty_polls is None)
        
        empty_poll_count = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        try:
            while True:
                try:
                    # Poll for messages
                    messages = self.poll_messages(
                        max_messages=1,
                        wait_time_seconds=poll_interval
                    )
                    
                    # Reset error counter on successful poll
                    consecutive_errors = 0
                    
                except Exception as poll_error:
                    consecutive_errors += 1
                    
                    # Check if it's a queue not found error
                    error_str = str(poll_error)
                    if "NonExistentQueue" in error_str:
                        logger.warning(
                            "sqs_queue_not_ready",
                            attempt=consecutive_errors,
                            max_attempts=max_consecutive_errors,
                            message="Queue not found. Waiting for LocalStack initialization..."
                        )
                        
                        # Wait before retrying (exponential backoff)
                        wait_time = min(2 ** consecutive_errors, 30)  # Max 30 seconds
                        logger.info("retrying_after_wait", wait_seconds=wait_time)
                        time.sleep(wait_time)
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(
                                "sqs_queue_still_not_found",
                                message="Queue not found after multiple retries. Please check LocalStack configuration."
                            )
                            raise
                        
                        continue
                    else:
                        # Other error - re-raise
                        raise
                
                messages = messages if 'messages' in locals() else []
                
                if not messages:
                    empty_poll_count += 1
                    logger.info(
                        "sqs_no_messages",
                        empty_poll_count=empty_poll_count,
                        max_empty_polls=max_empty_polls or "infinite"
                    )
                    
                    if max_empty_polls is not None and empty_poll_count >= max_empty_polls:
                        logger.info("sqs_polling_stopped_no_messages")
                        break
                    
                    continue
                
                # Reset empty poll counter
                empty_poll_count = 0
                
                # Process each message
                for message in messages:
                    try:
                        # Process and validate message
                        payload = self.process_message(message)
                        
                        if payload is None:
                            # Invalid message - delete it
                            self.delete_message(message['ReceiptHandle'])
                            continue
                        
                        # Handle message
                        success = message_handler(payload)
                        
                        if success:
                            # Delete message after successful processing
                            self.delete_message(message['ReceiptHandle'])
                        else:
                            logger.warning(
                                "sqs_message_handler_failed",
                                incident_id=payload.get('incident_id'),
                                message_id=message['MessageId']
                            )
                    
                    except Exception as e:
                        logger.error(
                            "sqs_message_processing_error",
                            error=str(e),
                            message_id=message.get('MessageId')
                        )
        
        except KeyboardInterrupt:
            logger.info("sqs_polling_interrupted")
        
        except Exception as e:
            logger.error("sqs_polling_error", error=str(e))
            raise

