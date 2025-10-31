"""Raw S3 storage with partitioned key structure for multi-signal data."""
import json
import gzip
import io
import boto3
from typing import Dict, Any, Optional, Literal
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger(__name__)

SignalType = Literal["logs", "metrics", "traces"]


class RawS3Storage:
    """Raw S3 storage with partitioned structure: env/tenant/service/signal/date/hour."""
    
    def __init__(self):
        """Initialize raw S3 storage client."""
        self.settings = get_settings()
        self.bucket_name = self.settings.s3_bucket_name
        
        # Configure S3 client
        if self.settings.is_local_environment:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.settings.localstack_endpoint,
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.aws_region
            )
        else:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.aws_region
            )
        
        # Ensure bucket exists (create if needed)
        self._ensure_bucket_exists()
        
        logger.info(
            "raw_s3_storage_initialized",
            bucket=self.bucket_name,
            region=self.settings.aws_region,
            is_local=self.settings.is_local_environment
        )
    
    def _ensure_bucket_exists(self) -> None:
        """Ensure S3 bucket exists, create if it doesn't."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("s3_bucket_exists", bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    if self.settings.aws_region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.settings.aws_region}
                        )
                    logger.info("s3_bucket_created", bucket=self.bucket_name)
                except Exception as create_error:
                    logger.error(
                        "failed_to_create_bucket",
                        bucket=self.bucket_name,
                        error=str(create_error)
                    )
            else:
                logger.warning(
                    "bucket_check_failed",
                    bucket=self.bucket_name,
                    error_code=error_code
                )
    
    def upload_raw_signal(
        self,
        data: Dict[str, Any],
        signal: SignalType,
        incident_id: str,
        start_ms: int,
        end_ms: int,
        sequence: int,
        part: int = 1,
        tenant: str = "default",
        service: str = "unknown",
        environment: str = "prod",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Upload raw signal data to S3 with partitioned key structure.
        
        Args:
            data: Raw SigNoz API response data
            signal: Signal type (logs, metrics, traces)
            incident_id: Incident identifier
            start_ms: Start time in epoch milliseconds
            end_ms: End time in epoch milliseconds
            sequence: Sequence number for this batch
            part: Part number within sequence (default: 1)
            tenant: Tenant/company identifier (default: "default")
            service: Service name (default: "unknown")
            environment: Environment (prod, stage, dev)
            metadata: Optional additional metadata
            
        Returns:
            S3 key of uploaded object
        """
        try:
            # Build S3 key with partitions
            s3_key = self._build_key(
                environment=environment,
                tenant=tenant,
                service=service,
                signal=signal,
                end_ms=end_ms,
                incident_id=incident_id,
                start_ms=start_ms,
                sequence=sequence,
                part=part
            )
            
            # Compress data
            compressed_data, original_size, compressed_size = self._compress_json(data)
            
            # Prepare metadata
            object_metadata = {
                "incident_id": incident_id,
                "signal": signal,
                "tenant": tenant,
                "service": service,
                "environment": environment,
                "start_ms": str(start_ms),
                "end_ms": str(end_ms),
                "sequence": str(sequence),
                "part": str(part),
                "original_size_bytes": str(original_size),
                "compressed_size_bytes": str(compressed_size),
                "compression_ratio": f"{compressed_size / original_size:.2%}" if original_size > 0 else "0%",
                "uploaded_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Add custom metadata if provided
            if metadata:
                for key, value in metadata.items():
                    if isinstance(value, (str, int, float, bool)):
                        object_metadata[f"custom_{key}"] = str(value)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata=object_metadata
            )
            
            logger.info(
                "raw_signal_uploaded_to_s3",
                incident_id=incident_id,
                signal=signal,
                s3_key=s3_key,
                original_size_kb=original_size / 1024,
                compressed_size_kb=compressed_size / 1024,
                compression_ratio=f"{compressed_size / original_size:.2%}" if original_size > 0 else "0%"
            )
            
            return s3_key
            
        except ClientError as e:
            logger.error(
                "s3_upload_failed",
                incident_id=incident_id,
                signal=signal,
                error=str(e),
                error_code=e.response['Error']['Code'],
                exc_info=True
            )
            raise Exception(f"Failed to upload {signal} to S3: {str(e)}")
            
        except Exception as e:
            logger.error(
                "raw_signal_upload_failed",
                incident_id=incident_id,
                signal=signal,
                error=str(e),
                exc_info=True
            )
            raise
    
    def upload_manifest(
        self,
        incident_id: str,
        signal: SignalType,
        manifest_data: Dict[str, Any],
        tenant: str = "default",
        service: str = "unknown",
        environment: str = "prod"
    ) -> str:
        """Upload manifest file for a signal batch.
        
        Args:
            incident_id: Incident identifier
            signal: Signal type
            manifest_data: Manifest metadata
            tenant: Tenant identifier
            service: Service name
            environment: Environment
            
        Returns:
            S3 key of uploaded manifest
        """
        try:
            # Build manifest key (same structure but with manifest.json suffix)
            timestamp = datetime.now(timezone.utc)
            date_str = timestamp.strftime("%Y-%m-%d")
            hour_str = timestamp.strftime("%H")
            
            s3_key = (
                f"raw/{environment}/{tenant}/{service}/{signal}/"
                f"date={date_str}/hour={hour_str}/"
                f"incident_id={incident_id}/manifest.json"
            )
            
            # Convert to JSON
            manifest_json = json.dumps(manifest_data, ensure_ascii=False, indent=2)
            
            # Upload manifest (uncompressed for easy reading)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=manifest_json.encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    "incident_id": incident_id,
                    "signal": signal,
                    "type": "manifest",
                    "uploaded_at": datetime.now(timezone.utc).isoformat()
                }
            )
            
            logger.info(
                "manifest_uploaded_to_s3",
                incident_id=incident_id,
                signal=signal,
                s3_key=s3_key
            )
            
            return s3_key
            
        except Exception as e:
            logger.error(
                "manifest_upload_failed",
                incident_id=incident_id,
                signal=signal,
                error=str(e),
                exc_info=True
            )
            raise
    
    def _build_key(
        self,
        environment: str,
        tenant: str,
        service: str,
        signal: SignalType,
        end_ms: int,
        incident_id: str,
        start_ms: int,
        sequence: int,
        part: int
    ) -> str:
        """Build S3 key with partitioned structure.
        
        Format: raw/{env}/{tenant}/{service}/{signal}/date={YYYY-MM-DD}/hour={HH}/
                incident_id={INCID}/window={start_ms}-{end_ms}/seq={seq}/part-{part}.json.gz
        
        Args:
            environment: Environment (prod, stage, dev)
            tenant: Tenant identifier
            service: Service name
            signal: Signal type
            end_ms: End time in epoch milliseconds
            incident_id: Incident identifier
            start_ms: Start time in epoch milliseconds
            sequence: Sequence number
            part: Part number
            
        Returns:
            S3 key string
        """
        # Convert end_ms to datetime for partitioning
        dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%H")
        
        key = (
            f"raw/{environment}/{tenant}/{service}/{signal}/"
            f"date={date_str}/hour={hour_str}/"
            f"incident_id={incident_id}/window={start_ms}-{end_ms}/"
            f"seq={sequence:05d}/part-{part:04d}.json.gz"
        )
        
        return key
    
    def _compress_json(self, data: Dict[str, Any]) -> tuple[bytes, int, int]:
        """Compress JSON data using gzip.
        
        Args:
            data: Data to compress
            
        Returns:
            Tuple of (compressed_bytes, original_size, compressed_size)
        """
        try:
            # Convert to JSON
            json_data = json.dumps(data, ensure_ascii=False)
            original_bytes = json_data.encode('utf-8')
            original_size = len(original_bytes)
            
            # Compress with gzip
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as gz:
                gz.write(original_bytes)
            
            compressed_bytes = buf.getvalue()
            compressed_size = len(compressed_bytes)
            
            return compressed_bytes, original_size, compressed_size
            
        except Exception as e:
            logger.error("json_compression_failed", error=str(e))
            raise
    
    def list_incident_data(
        self,
        incident_id: str,
        signal: Optional[SignalType] = None,
        tenant: str = "default",
        service: str = "unknown",
        environment: str = "prod"
    ) -> list[str]:
        """List all S3 keys for an incident.
        
        Args:
            incident_id: Incident identifier
            signal: Optional signal type filter
            tenant: Tenant identifier
            service: Service name
            environment: Environment
            
        Returns:
            List of S3 keys
        """
        try:
            # Build prefix
            if signal:
                prefix = f"raw/{environment}/{tenant}/{service}/{signal}/"
            else:
                prefix = f"raw/{environment}/{tenant}/{service}/"
            
            # List objects
            keys = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if f"incident_id={incident_id}" in key:
                            keys.append(key)
            
            logger.info(
                "incident_data_listed",
                incident_id=incident_id,
                signal=signal,
                count=len(keys)
            )
            
            return keys
            
        except Exception as e:
            logger.error(
                "list_incident_data_failed",
                incident_id=incident_id,
                error=str(e),
                exc_info=True
            )
            return []

