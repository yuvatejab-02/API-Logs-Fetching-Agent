"""Configuration management using Pydantic settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # AWS Bedrock Configuration
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "us-east-1"
    bedrock_model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0"
    
    # SigNoz Configuration
    signoz_api_endpoint: str
    signoz_api_key: str
    
    # S3/LocalStack
    s3_bucket_name: str = "incident-logs"
    localstack_endpoint: Optional[str] = None
    use_localstack: bool = False
    
    # SQS Configuration
    sqs_input_queue_url: Optional[str] = None
    sqs_output_queue_url: Optional[str] = None
    sqs_enabled: bool = False
    sqs_poll_interval: int = 20
    sqs_max_empty_polls: Optional[int] = None  # None = infinite polling
    sqs_visibility_timeout: int = 300
    
    # Application
    log_level: str = "INFO"
    polling_duration_minutes: int = 30
    polling_interval_seconds: int = 30
    
    @property
    def is_local_environment(self) -> bool:
        """Check if running in local/development environment."""
        return self.use_localstack and self.localstack_endpoint is not None
    
    @property
    def bedrock_endpoint_url(self) -> Optional[str]:
        """Get Bedrock endpoint URL (None for AWS, custom for LocalStack)."""
        # Note: LocalStack doesn't fully support Bedrock, so we use real AWS
        return None


def get_settings() -> Settings:
    """Get application settings instance."""
    return Settings()
