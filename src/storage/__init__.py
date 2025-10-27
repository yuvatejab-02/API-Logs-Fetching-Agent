"""Storage module for saving logs."""
from .local_storage import LocalStorage
from .s3_storage import S3Storage

__all__ = ["LocalStorage", "S3Storage"]
