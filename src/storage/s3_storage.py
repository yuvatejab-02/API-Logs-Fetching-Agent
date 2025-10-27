"""S3 storage handler for both LocalStack and AWS."""
import json
import boto3
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class S3Storage:
    """Handle S3 storage for logs (works with LocalStack and AWS)."""
    
    def __init__(self):
        """Initialize S3 storage client."""
        settings = get_settings()
        
        # Configure S3 client
        s3_config = {
            'aws_access_key_id': settings.aws_access_key_id,
            'aws_secret_access_key': settings.aws_secret_access_key,
            'region_name': settings.aws_region
        }
        
        # Add LocalStack endpoint if in local mode
        if settings.is_local_environment:
            s3_config['endpoint_url'] = settings.localstack_endpoint
            logger.info(
                "s3_storage_initialized_localstack",
                endpoint=settings.localstack_endpoint
            )
        else:
            logger.info("s3_storage_initialized_aws")
        
        self.s3_client = boto3.client('s3', **s3_config)
        self.bucket_name = settings.s3_bucket_name
        self.is_local = settings.is_local_environment
        
        # Verify bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the S3 bucket exists, create if not."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"s3_bucket_verified", bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.warning(f"s3_bucket_not_found", bucket=self.bucket_name)
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"s3_bucket_created", bucket=self.bucket_name)
                except ClientError as create_error:
                    logger.error(
                        "s3_bucket_creation_failed",
                        bucket=self.bucket_name,
                        error=str(create_error)
                    )
                    raise
            else:
                logger.error("s3_bucket_check_failed", error=str(e))
                raise
    
    def upload_logs(
        self,
        logs: List[Dict[str, Any]],
        incident_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        file_type: str = "logs"
    ) -> str:
        """Upload logs to S3.
        
        Args:
            logs: List of log entries
            incident_id: Incident identifier
            metadata: Optional metadata about the logs
            file_type: Type of file (logs, final_aggregated, raw)
            
        Returns:
            S3 key (path) of uploaded file
        """
        # Generate S3 key with hierarchical structure
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = self._generate_s3_key(
            incident_id=incident_id,
            file_type=file_type,
            timestamp=timestamp
        )
        
        # Prepare data
        upload_data = {
            "metadata": {
                "incident_id": incident_id,
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
                "log_count": len(logs),
                "file_type": file_type,
                "storage_backend": "localstack" if self.is_local else "aws_s3",
                **(metadata or {})
            },
            "logs": logs
        }
        
        # Convert to JSON
        json_data = json.dumps(upload_data, indent=2, ensure_ascii=False)
        
        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'incident_id': incident_id,
                    'log_count': str(len(logs)),
                    'file_type': file_type
                }
            )
            
            logger.info(
                "logs_uploaded_to_s3",
                incident_id=incident_id,
                s3_key=s3_key,
                log_count=len(logs),
                bucket=self.bucket_name
            )
            
            return s3_key
            
        except ClientError as e:
            logger.error(
                "s3_upload_failed",
                incident_id=incident_id,
                s3_key=s3_key,
                error=str(e)
            )
            raise Exception(f"Failed to upload to S3: {str(e)}")
    
    def _generate_s3_key(
        self,
        incident_id: str,
        file_type: str,
        timestamp: str
    ) -> str:
        """Generate S3 key with hierarchical structure.
        
        Structure: incidents/{incident_id}/{file_type}/{timestamp}.json
        
        Args:
            incident_id: Incident identifier
            file_type: Type of file
            timestamp: Timestamp string
            
        Returns:
            S3 key path
        """
        return f"incidents/{incident_id}/{file_type}/{timestamp}.json"
    
    def list_incident_files(self, incident_id: str) -> List[Dict[str, Any]]:
        """List all files for an incident.
        
        Args:
            incident_id: Incident identifier
            
        Returns:
            List of file information dictionaries
        """
        prefix = f"incidents/{incident_id}/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    })
            
            logger.info(
                "incident_files_listed",
                incident_id=incident_id,
                file_count=len(files)
            )
            
            return files
            
        except ClientError as e:
            logger.error(
                "s3_list_failed",
                incident_id=incident_id,
                error=str(e)
            )
            return []
    
    def download_logs(self, s3_key: str) -> Dict[str, Any]:
        """Download logs from S3.
        
        Args:
            s3_key: S3 key of the file
            
        Returns:
            Dictionary containing metadata and logs
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            
            logger.info(
                "logs_downloaded_from_s3",
                s3_key=s3_key,
                log_count=len(data.get('logs', []))
            )
            
            return data
            
        except ClientError as e:
            logger.error(
                "s3_download_failed",
                s3_key=s3_key,
                error=str(e)
            )
            raise Exception(f"Failed to download from S3: {str(e)}")
