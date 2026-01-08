#!/bin/bash
# E2E tests for AWS Signature V4 authentication

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
export AUTH_SERVER_PORT=9091
export AUTH_SERVER_ADDR="http://localhost:${AUTH_SERVER_PORT}"
export AUTH_TEST_BUCKET="test-auth-bucket-e2e"
export AUTH_TEST_DATA_DIR=$(mktemp -d)
export AUTH_SERVER_DATA_DIR=$(mktemp -d)
export AUTH_SERVER_PID=""

# Set restrictive permissions on temporary directories
chmod 700 "${AUTH_TEST_DATA_DIR}"
chmod 700 "${AUTH_SERVER_DATA_DIR}"

# Test credentials
export TEST_ACCESS_KEY="test-access-key-e2e"
export TEST_SECRET_KEY="test-secret-key-e2e"

# Cleanup function
cleanup_auth() {
    echo -e "\n${YELLOW}Cleaning up auth tests...${NC}"
    if [ -n "$AUTH_SERVER_PID" ]; then
        kill -TERM $AUTH_SERVER_PID 2>/dev/null || true
        sleep 1
        kill -KILL $AUTH_SERVER_PID 2>/dev/null || true
        wait $AUTH_SERVER_PID 2>/dev/null || true
    fi
    rm -rf "${AUTH_TEST_DATA_DIR}"
    rm -rf "${AUTH_SERVER_DATA_DIR}"
}

# Setup function
setup_auth() {
    echo -e "${YELLOW}Starting S3-compatible server with authentication...${NC}"
    echo "Server address: ${AUTH_SERVER_ADDR}"
    echo "Test data directory: ${AUTH_TEST_DATA_DIR}"
    echo "Server data directory: ${AUTH_SERVER_DATA_DIR}"
    echo "Access Key: ${TEST_ACCESS_KEY}"

    trap cleanup_auth EXIT

    # Build the server if not already built
    if [ ! -f "./s3d" ]; then
        echo -e "\n${YELLOW}Building server...${NC}"
        go build -o ./s3d ./cmd/s3d
        if [ $? -ne 0 ]; then
            echo -e "${RED}Failed to build server${NC}"
            exit 1
        fi
        echo -e "${GREEN}Server built successfully${NC}"
    fi

    # Start the server with authentication
    echo -e "\n${YELLOW}Starting server with authentication...${NC}"
    ./s3d -addr ":${AUTH_SERVER_PORT}" -data "${AUTH_SERVER_DATA_DIR}" -credentials "${TEST_ACCESS_KEY}:${TEST_SECRET_KEY}" > /dev/null 2>&1 &
    AUTH_SERVER_PID=$!
    echo "Server PID: ${AUTH_SERVER_PID}"

    # Wait for server to start
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "${AUTH_SERVER_ADDR}" > /dev/null 2>&1; then
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
        echo "See: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi

    echo -e "\n${YELLOW}AWS CLI version:${NC}"
    aws --version

    # Configure AWS CLI with test credentials
    export AWS_ACCESS_KEY_ID=${TEST_ACCESS_KEY}
    export AWS_SECRET_ACCESS_KEY=${TEST_SECRET_KEY}
    export AWS_DEFAULT_REGION=us-east-1
}

# Test 1: Request without credentials should fail
test_auth_no_credentials() {
    echo -e "\n${YELLOW}Test: Request without credentials (should fail)${NC}"
    
    # Temporarily unset credentials
    local OLD_ACCESS_KEY=$AWS_ACCESS_KEY_ID
    local OLD_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
    unset AWS_ACCESS_KEY_ID
    unset AWS_SECRET_ACCESS_KEY
    
    # This should fail with authentication error (without --no-sign-request, it will try to sign with no credentials)
    # We expect a 403 Forbidden error
    if curl -s -o /dev/null -w "%{http_code}" "${AUTH_SERVER_ADDR}/" | grep -q "403"; then
        echo -e "${GREEN}✓ Request correctly rejected without credentials${NC}"
    else
        # Restore credentials before exiting
        export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
        export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
        echo -e "${RED}✗ Request should have been rejected${NC}"
        exit 1
    fi
    
    # Restore credentials
    export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
}

# Test 2: Request with wrong credentials should fail
test_auth_wrong_credentials() {
    echo -e "\n${YELLOW}Test: Request with wrong credentials (should fail)${NC}"
    
    # Temporarily use wrong credentials
    local OLD_ACCESS_KEY=$AWS_ACCESS_KEY_ID
    local OLD_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
    export AWS_ACCESS_KEY_ID="wrong-access-key"
    export AWS_SECRET_ACCESS_KEY="wrong-secret-key"
    
    # This should fail with authentication error
    if aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 ls 2>&1 | grep -q "403\|Forbidden\|SignatureDoesNotMatch\|InvalidAccessKeyId"; then
        echo -e "${GREEN}✓ Request correctly rejected with wrong credentials${NC}"
    else
        # Restore credentials before exiting
        export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
        export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
        echo -e "${RED}✗ Request should have been rejected${NC}"
        exit 1
    fi
    
    # Restore credentials
    export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
}

# Test 3: Request with valid credentials should succeed
test_auth_valid_credentials() {
    echo -e "\n${YELLOW}Test: Request with valid credentials (should succeed)${NC}"
    
    # This should succeed
    if aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 ls > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Request succeeded with valid credentials${NC}"
    else
        echo -e "${RED}✗ Request should have succeeded${NC}"
        exit 1
    fi
}

# Test 4: Create bucket with authentication
test_auth_create_bucket() {
    echo -e "\n${YELLOW}Test: Create bucket with authentication${NC}"
    aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 mb s3://${AUTH_TEST_BUCKET}
    echo -e "${GREEN}✓ Bucket created with authentication${NC}"
}

# Test 5: Upload object with authentication
test_auth_upload_object() {
    echo -e "\n${YELLOW}Test: Upload object with authentication${NC}"
    echo "Test content with auth" > "${AUTH_TEST_DATA_DIR}/test-file.txt"
    aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 cp "${AUTH_TEST_DATA_DIR}/test-file.txt" s3://${AUTH_TEST_BUCKET}/test-file.txt
    echo -e "${GREEN}✓ Object uploaded with authentication${NC}"
}

# Test 6: Download object with authentication
test_auth_download_object() {
    echo -e "\n${YELLOW}Test: Download object with authentication${NC}"
    aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 cp s3://${AUTH_TEST_BUCKET}/test-file.txt "${AUTH_TEST_DATA_DIR}/downloaded.txt"
    
    if diff "${AUTH_TEST_DATA_DIR}/test-file.txt" "${AUTH_TEST_DATA_DIR}/downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ Object downloaded correctly with authentication${NC}"
    else
        echo -e "${RED}✗ Downloaded content doesn't match${NC}"
        exit 1
    fi
}

# Test 7: List objects with authentication
test_auth_list_objects() {
    echo -e "\n${YELLOW}Test: List objects with authentication${NC}"
    OBJECTS=$(aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 ls s3://${AUTH_TEST_BUCKET}/)
    if echo "$OBJECTS" | grep -q "test-file.txt"; then
        echo -e "${GREEN}✓ Objects listed successfully with authentication${NC}"
    else
        echo -e "${RED}✗ Object not found in list${NC}"
        exit 1
    fi
}

# Test 8: Delete object with authentication
test_auth_delete_object() {
    echo -e "\n${YELLOW}Test: Delete object with authentication${NC}"
    aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 rm s3://${AUTH_TEST_BUCKET}/test-file.txt
    echo -e "${GREEN}✓ Object deleted with authentication${NC}"
}

# Test 9: Delete bucket with authentication
test_auth_delete_bucket() {
    echo -e "\n${YELLOW}Test: Delete bucket with authentication${NC}"
    aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 rb s3://${AUTH_TEST_BUCKET}
    BUCKETS=$(aws --endpoint-url="${AUTH_SERVER_ADDR}" s3 ls)
    if ! echo "$BUCKETS" | grep -q "${AUTH_TEST_BUCKET}"; then
        echo -e "${GREEN}✓ Bucket deleted with authentication${NC}"
    else
        echo -e "${RED}✗ Bucket still exists${NC}"
        exit 1
    fi
}

# Main execution
main() {
    setup_auth
    
    test_auth_no_credentials
    test_auth_wrong_credentials
    test_auth_valid_credentials
    test_auth_create_bucket
    test_auth_upload_object
    test_auth_download_object
    test_auth_list_objects
    test_auth_delete_object
    test_auth_delete_bucket
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All authentication e2e tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main
fi
