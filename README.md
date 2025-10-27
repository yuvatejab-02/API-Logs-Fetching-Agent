# LocalStack Test Infrastructure

## Quick Start

1. **Start LocalStack:**
cd tests/localstack
docker-compose up -d

text

2. **Setup resources:**
python setup_localstack.py

text

3. **Verify:**
Check containers
docker ps | grep incident-analyzer-localstack

Check health
curl http://localhost:14566/_localstack/health

text

## Cleanup

Stop LocalStack
docker-compose down

Clean data
rm -rf volume/

Remove containers
docker-compose down -v