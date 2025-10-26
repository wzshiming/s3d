#!/bin/bash
# Common setup and configuration for e2e tests

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
export SERVER_PORT=9090
export SERVER_ADDR="http://localhost:${SERVER_PORT}"
export TEST_BUCKET="test-bucket-e2e"
export TEST_DATA_DIR=$(mktemp -d)
export SERVER_DATA_DIR=$(mktemp -d)
export SERVER_PID=""

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    rm -rf "${TEST_DATA_DIR}"
    rm -rf "${SERVER_DATA_DIR}"
}

# Setup function
setup() {
    echo -e "${YELLOW}Starting S3-compatible server e2e test...${NC}"
    echo "Server address: ${SERVER_ADDR}"
    echo "Test data directory: ${TEST_DATA_DIR}"
    echo "Server data directory: ${SERVER_DATA_DIR}"

    trap cleanup EXIT

    # Build the server
    echo -e "\n${YELLOW}Building server...${NC}"
    go build -o ./s3d ./cmd/s3d
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build server${NC}"
        exit 1
    fi
    echo -e "${GREEN}Server built successfully${NC}"

    # Start the server in the background without authentication
    echo -e "\n${YELLOW}Starting server (no authentication mode)...${NC}"
    ./s3d -addr ":${SERVER_PORT}" -data "${SERVER_DATA_DIR}" > /dev/null 2>&1 &
    SERVER_PID=$!
    echo "Server PID: ${SERVER_PID}"

    # Wait for server to start
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "${SERVER_ADDR}" > /dev/null 2>&1; then
            echo -e "${GREEN}Server is ready${NC}"
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${RED}Server failed to start${NC}"
            exit 1
        fi
        sleep 1
    done

    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}AWS CLI is not installed. Please install it first.${NC}"
        echo "Install with: pip install awscli"
        exit 1
    fi

    echo -e "\n${YELLOW}AWS CLI version:${NC}"
    aws --version

    # Configure AWS CLI with dummy credentials
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_DEFAULT_REGION=us-east-1
}
