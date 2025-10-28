## Local Testing Qucik start

1. **Start LocalStack:**
cd tests/localstack
docker-compose up -d

2. **Setup resources:**
python setup_localstack.py

3. **Verify:**
Check containers

docker ps | grep incident-analyzer-localstack

4. **Check health**

curl http://localhost:14566/_localstack/health

5. **Run main file**
   
run python -m src.main --incident-file tests/test_data/sample_payloads.json

6. **Cleanup**

Stop LocalStack

docker-compose down


## Docker Testing Quick Start

1. **Run complete test environment**
   
docker-compose -f docker-compose.test.yml up --build

2. **Check results in S3 bucket**
   
http://localhost:14566/incident-logs-test

3. **Clean Up**
   
docker-compose -f docker-compose.test.yml down


## Run Testing scripts

```
tests/
├── conftest.py                  # Shared fixtures (if needed)
├── test_data/
│   └── sample_payloads.json    # Test data
├── reports/                     # Generated reports
├── test_llm_query.py           # ✅ Test 1: LLM
├── test_signoz_fetch.py        # ✅ Test 2: SigNoz
├── test_s3_storage.py          # ✅ Test 3: S3
└── test_e2e.py                 # ✅ Test 4: E2E
```

1 **Run from root**

cd C:\Users\yuvat\OneDrive\Documents\incident-log-analyser

2. **Run all tests**
   
pytest tests/ -v

3. **Run only E2E test**
   
pytest tests/test_e2e.py -v -s

4. Make sure LocalStack is running for e2e, s3 testing

docker-compose -f docker-compose.test.yml up -d localstack

5. **Run all tests with detailed output**
   
pytest tests/ -v -s

This will generate a comprehensive report showing the complete data flow through all 6 steps!
