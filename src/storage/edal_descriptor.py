"""EDAL datasource descriptor generator for S3 data sources."""
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from ..utils.config import get_settings
from ..utils.logger import get_logger

logger = get_logger(__name__)


class EDALDescriptorGenerator:
    """Generate EDAL-compatible datasource descriptors for S3 storage."""
    
    def __init__(self):
        """Initialize descriptor generator."""
        self.settings = get_settings()
        logger.info("edal_descriptor_generator_initialized")
    
    def generate_descriptor(
        self,
        company_id: str,
        tenant: str,
        service: str,
        environment: str = "prod",
        signals: Optional[List[str]] = None,
        use_iam_role: bool = True,
        role_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate EDAL datasource descriptor for incident data.
        
        Args:
            company_id: Company/tenant identifier
            tenant: Tenant identifier for S3 path
            service: Service name
            environment: Environment (prod, stage, dev)
            signals: List of signals to include (default: ["logs", "traces"])
            use_iam_role: Use IAM role auth instead of access keys (default: True)
            role_arn: IAM role ARN (required if use_iam_role=True)
            
        Returns:
            EDAL datasource descriptor dictionary
        """
        signals = signals or ["logs", "traces"]
        
        data_sources = []
        
        for signal in signals:
            source_id = f"s3-{environment}-{signal}"
            path_prefix = f"raw/{environment}/{tenant}/{service}/{signal}"
            
            # Build connection info
            connection_info = {
                "bucket_name": self.settings.s3_bucket_name,
                "region": self.settings.aws_region,
                "path_prefix": path_prefix
            }
            
            # Build auth config
            if use_iam_role:
                if not role_arn:
                    raise ValueError("role_arn is required when use_iam_role=True")
                auth_config = {
                    "auth_type": "iam_role",
                    "role_arn": role_arn
                }
            else:
                # Note: In production, never store access keys in descriptors
                # This is for reference only - use secrets manager
                auth_config = {
                    "auth_type": "access_key",
                    "access_key_id": "${SECRET:AWS_ACCESS_KEY_ID}",
                    "secret_access_key": "${SECRET:AWS_SECRET_ACCESS_KEY}"
                }
            
            data_source = {
                "source_id": source_id,
                "source_type": "s3",
                "connection_info": connection_info,
                "auth_config": auth_config,
                "metadata": {
                    "signal": signal,
                    "environment": environment,
                    "service": service,
                    "tenant": tenant,
                    "format": "json.gz",
                    "schema_version": "v1"
                }
            }
            
            data_sources.append(data_source)
        
        descriptor = {
            "company_id": company_id,
            "data_sources": data_sources,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0"
        }
        
        logger.info(
            "edal_descriptor_generated",
            company_id=company_id,
            tenant=tenant,
            service=service,
            environment=environment,
            signals=signals,
            source_count=len(data_sources)
        )
        
        return descriptor
    
    def save_descriptor_to_s3(
        self,
        descriptor: Dict[str, Any],
        s3_client,
        bucket_name: str,
        tenant: str,
        environment: str = "prod"
    ) -> str:
        """Save EDAL descriptor to S3.
        
        Args:
            descriptor: EDAL descriptor dictionary
            s3_client: Boto3 S3 client
            bucket_name: S3 bucket name
            tenant: Tenant identifier
            environment: Environment
            
        Returns:
            S3 key of saved descriptor
        """
        try:
            # Build S3 key for descriptor
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            s3_key = f"config/{environment}/{tenant}/edal/datasources_{timestamp}.json"
            
            # Convert to JSON
            descriptor_json = json.dumps(descriptor, ensure_ascii=False, indent=2)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=descriptor_json.encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    "type": "edal_descriptor",
                    "tenant": tenant,
                    "environment": environment,
                    "company_id": descriptor.get("company_id", "unknown"),
                    "version": descriptor.get("version", "1.0"),
                    "uploaded_at": datetime.now(timezone.utc).isoformat()
                }
            )
            
            logger.info(
                "edal_descriptor_saved_to_s3",
                s3_key=s3_key,
                tenant=tenant,
                environment=environment
            )
            
            return s3_key
            
        except Exception as e:
            logger.error(
                "edal_descriptor_save_failed",
                error=str(e),
                tenant=tenant,
                environment=environment,
                exc_info=True
            )
            raise
    
    def generate_and_save(
        self,
        company_id: str,
        tenant: str,
        service: str,
        s3_client,
        environment: str = "prod",
        signals: Optional[List[str]] = None,
        use_iam_role: bool = True,
        role_arn: Optional[str] = None
    ) -> tuple[Dict[str, Any], str]:
        """Generate and save EDAL descriptor in one step.
        
        Args:
            company_id: Company identifier
            tenant: Tenant identifier
            service: Service name
            s3_client: Boto3 S3 client
            environment: Environment
            signals: List of signals
            use_iam_role: Use IAM role auth
            role_arn: IAM role ARN
            
        Returns:
            Tuple of (descriptor_dict, s3_key)
        """
        descriptor = self.generate_descriptor(
            company_id=company_id,
            tenant=tenant,
            service=service,
            environment=environment,
            signals=signals,
            use_iam_role=use_iam_role,
            role_arn=role_arn
        )
        
        s3_key = self.save_descriptor_to_s3(
            descriptor=descriptor,
            s3_client=s3_client,
            bucket_name=self.settings.s3_bucket_name,
            tenant=tenant,
            environment=environment
        )
        
        return descriptor, s3_key


