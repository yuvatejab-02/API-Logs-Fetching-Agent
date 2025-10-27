## Local Testing Qucik start

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

4. **Run main file**
run python -m src.main --incident-file tests/test_data/sample_payloads.json

5. **Cleanup**

Stop LocalStack
docker-compose down


## Docker Testing Quick Start

1. **Run complete test environment**
docker-compose -f docker-compose.test.yml up --build

2. **Check results in S3 bucket**
http://localhost:14566/incident-logs-test

3. **Clean Up**
docker-compose -f docker-compose.test.yml down