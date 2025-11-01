# Changelog

All notable changes to this project will be documented in this file.

## [2.0.0] - 2025-11-01

### 🚀 Major Changes (Breaking)

#### New Input Payload Format
- **BREAKING**: Input payload now requires `data_sources` array with SigNoz credentials
- SigNoz API endpoint and key are now provided via payload instead of environment variables
- Added `company_id` as required field in incident section

#### New Output Payload Format
- **BREAKING**: Output payload simplified to only include incident metadata and S3 URLs
- Output now contains `incident` object with `incident_id`, `company_id`, `service`, and `env`
- Output now contains `sources.signoz` object with full HTTPS S3 URLs for logs, traces, and metrics

### ✨ Added
- Added JSON schemas for input/output payload validation
- Added environment-specific configuration examples (dev, local, prod)
- Added comprehensive payload validation tests
- Added S3 URL generation method for full HTTPS URLs
- Added SQS retry mechanism with exponential backoff

### 🔧 Changed
- Updated SigNoz clients to accept dynamic API credentials
- Updated output message format to new simplified structure
- Updated Docker Compose for better LocalStack integration

### 🐛 Fixed
- Fixed potential NoneType error in SigNoz client initialization
- Fixed SQS queue initialization timing issues
- Fixed S3 URL generation for AWS and LocalStack

### 🗑️ Removed
- Removed unused imports and files
- Removed old output payload format

## [1.0.0] - 2025-10-30

### Initial Release
- Basic incident log analysis functionality
- SigNoz integration for logs, traces, and metrics
- AWS Bedrock LLM integration
- S3 storage and EDAL descriptor generation
- SQS integration
