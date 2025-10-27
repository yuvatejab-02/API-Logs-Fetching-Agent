#!/bin/bash
# Comprehensive Docker test script

set -e

echo "==============================================="
echo "  🚀 INCIDENT LOG ANALYZER - DOCKER E2E TEST"
echo "==============================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check Docker
echo -e "${YELLOW}Checking Docker...${NC}"
if ! docker --version > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker not found${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker found${NC}"

# Check .env file
echo -e "${YELLOW}Checking .env file...${NC}"
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env file not found${NC}"
    echo "Please create .env from .env.example"
    exit 1
fi
echo -e "${GREEN}✅ .env file found${NC}"

# Build image
echo ""
echo -e "${YELLOW}Building Docker image...${NC}"
docker build -t incident-log-analyzer:test .
echo -e "${GREEN}✅ Image built${NC}"

# Start LocalStack
echo ""
echo -e "${YELLOW}Starting LocalStack...${NC}"
docker-compose -f docker-compose.test.yml up -d localstack
sleep 5

# Wait for LocalStack
echo -e "${YELLOW}Waiting for LocalStack...${NC}"
until curl -s http://localhost:14566/_localstack/health | grep -q "running"; do
    echo "   Waiting..."
    sleep 2
done
echo -e "${GREEN}✅ LocalStack ready${NC}"

# Setup S3 bucket
echo ""
echo -e "${YELLOW}Setting up S3 bucket...${NC}"
docker run --rm --network incident-log-analyser_test-network \
    -e AWS_ACCESS_KEY_ID=test \
    -e AWS_SECRET_ACCESS_KEY=test \
    amazon/aws-cli --endpoint-url=http://localstack:4566 \
    s3 mb s3://incident-logs-test 2>/dev/null || echo "Bucket exists"
echo -e "${GREEN}✅ S3 bucket ready${NC}"

# Run test
echo ""
echo -e "${YELLOW}Running incident analyzer...${NC}"
docker run --rm \
    --network incident-log-analyser_test-network \
    -v $(pwd)/output:/app/output \
    -v $(pwd)/tests/test_data:/app/test_data \
    --env-file .env \
    -e USE_LOCALSTACK=true \
    -e LOCALSTACK_ENDPOINT=http://localstack:4566 \
    -e POLLING_DURATION_MINUTES=2 \
    incident-log-analyzer:test \
    --incident-file /app/test_data/sample_payloads.json

# Check results
echo ""
echo -e "${YELLOW}Checking results...${NC}"
if [ -d "output/unknown" ]; then
    echo -e "${GREEN}✅ Logs saved locally${NC}"
    echo "   Files: $(ls -1 output/unknown/ | wc -l)"
else
    echo -e "${RED}❌ No local logs found${NC}"
fi

# Check S3
echo -e "${YELLOW}Checking S3 storage...${NC}"
docker run --rm --network incident-log-analyser_test-network \
    -e AWS_ACCESS_KEY_ID=test \
    -e AWS_SECRET_ACCESS_KEY=test \
    amazon/aws-cli --endpoint-url=http://localstack:4566 \
    s3 ls s3://incident-logs-test/incidents/ --recursive

# Cleanup
echo ""
echo -e "${YELLOW}Cleanup? (y/n)${NC}"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "Cleaning up..."
    docker-compose -f docker-compose.test.yml down -v
    docker rmi incident-log-analyzer:test
    echo -e "${GREEN}✅ Cleanup complete${NC}"
fi

echo ""
echo "==============================================="
echo -e "${GREEN}  ✅ DOCKER E2E TEST COMPLETE${NC}"
echo "==============================================="
