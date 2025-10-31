"""Storage module for saving logs."""
from .local_storage import LocalStorage
from .raw_s3_storage import RawS3Storage
from .edal_descriptor import EDALDescriptorGenerator

__all__ = ["LocalStorage", "RawS3Storage", "EDALDescriptorGenerator"]
