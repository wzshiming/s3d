#!/bin/bash
# E2E tests for AWS SigV4 chunked upload (streaming) support

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
export CHUNKED_SERVER_PORT=9092
export CHUNKED_SERVER_ADDR="http://localhost:${CHUNKED_SERVER_PORT}"
export CHUNKED_TEST_BUCKET="test-chunked-bucket-e2e"
export CHUNKED_TEST_DATA_DIR=$(mktemp -d)
export CHUNKED_SERVER_DATA_DIR=$(mktemp -d)
export CHUNKED_SERVER_PID=""

# Set restrictive permissions on temporary directories
chmod 700 "${CHUNKED_TEST_DATA_DIR}"
chmod 700 "${CHUNKED_SERVER_DATA_DIR}"

# Test credentials
export TEST_ACCESS_KEY="test-chunked-access-key"
export TEST_SECRET_KEY="test-chunked-secret-key"

# Cleanup function
cleanup_chunked() {
    echo -e "\n${YELLOW}Cleaning up chunked tests...${NC}"
    if [ -n "$CHUNKED_SERVER_PID" ]; then
        kill -TERM $CHUNKED_SERVER_PID 2>/dev/null || true
        sleep 1
        kill -KILL $CHUNKED_SERVER_PID 2>/dev/null || true
        wait $CHUNKED_SERVER_PID 2>/dev/null || true
    fi
    rm -rf "${CHUNKED_TEST_DATA_DIR}"
    rm -rf "${CHUNKED_SERVER_DATA_DIR}"
}

# Setup function
setup_chunked() {
    echo -e "${YELLOW}Starting S3-compatible server for chunked upload tests...${NC}"
    echo "Server address: ${CHUNKED_SERVER_ADDR}"
    echo "Test data directory: ${CHUNKED_TEST_DATA_DIR}"
    echo "Server data directory: ${CHUNKED_SERVER_DATA_DIR}"
    echo "Access Key: ${TEST_ACCESS_KEY}"

    trap cleanup_chunked EXIT

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
    ./s3d -addr ":${CHUNKED_SERVER_PORT}" -data "${CHUNKED_SERVER_DATA_DIR}" -credentials "${TEST_ACCESS_KEY}:${TEST_SECRET_KEY}" > /dev/null 2>&1 &
    CHUNKED_SERVER_PID=$!
    echo "Server PID: ${CHUNKED_SERVER_PID}"

    # Wait for server to start
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "${CHUNKED_SERVER_ADDR}" > /dev/null 2>&1; then
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

# Test 1: Create bucket for chunked tests
test_chunked_create_bucket() {
    echo -e "\n${YELLOW}Test: Create bucket for chunked upload tests${NC}"
    aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 mb s3://${CHUNKED_TEST_BUCKET}
    echo -e "${GREEN}✓ Bucket created${NC}"
}

# Test 2: Upload file using chunked transfer (streaming stdin)
test_chunked_upload_stdin() {
    echo -e "\n${YELLOW}Test: Upload file using chunked transfer (streaming stdin)${NC}"
    
    # Create test content
    TEST_CONTENT="This is test content for chunked upload via stdin"
    
    # Upload using stdin - this forces chunked transfer encoding
    echo "$TEST_CONTENT" | aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp - s3://${CHUNKED_TEST_BUCKET}/chunked-stdin.txt
    
    # Verify the file was uploaded
    if aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 ls s3://${CHUNKED_TEST_BUCKET}/ | grep -q "chunked-stdin.txt"; then
        echo -e "${GREEN}✓ Chunked upload via stdin successful${NC}"
    else
        echo -e "${RED}✗ Chunked upload via stdin failed${NC}"
        exit 1
    fi
    
    # Download and verify content
    DOWNLOADED=$(aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp s3://${CHUNKED_TEST_BUCKET}/chunked-stdin.txt -)
    if [ "$DOWNLOADED" = "$TEST_CONTENT" ]; then
        echo -e "${GREEN}✓ Content verified after chunked upload${NC}"
    else
        echo -e "${RED}✗ Content mismatch after chunked upload${NC}"
        echo "Expected: $TEST_CONTENT"
        echo "Got: $DOWNLOADED"
        exit 1
    fi
}

# Test 3: Upload larger content via stdin (multiple chunks)
test_chunked_upload_large_stdin() {
    echo -e "\n${YELLOW}Test: Upload larger content via stdin (multiple chunks)${NC}"
    
    # Create 1MB of test content
    dd if=/dev/urandom bs=1024 count=1024 2>/dev/null | base64 > "${CHUNKED_TEST_DATA_DIR}/large-content.txt"
    ORIGINAL_HASH=$(cat "${CHUNKED_TEST_DATA_DIR}/large-content.txt" | sha256sum | cut -d' ' -f1)
    
    # Upload using stdin with larger content
    cat "${CHUNKED_TEST_DATA_DIR}/large-content.txt" | aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp - s3://${CHUNKED_TEST_BUCKET}/chunked-large.txt
    
    # Verify the file was uploaded
    if aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 ls s3://${CHUNKED_TEST_BUCKET}/ | grep -q "chunked-large.txt"; then
        echo -e "${GREEN}✓ Large chunked upload successful${NC}"
    else
        echo -e "${RED}✗ Large chunked upload failed${NC}"
        exit 1
    fi
    
    # Download and verify content hash
    aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp s3://${CHUNKED_TEST_BUCKET}/chunked-large.txt "${CHUNKED_TEST_DATA_DIR}/downloaded-large.txt"
    DOWNLOADED_HASH=$(cat "${CHUNKED_TEST_DATA_DIR}/downloaded-large.txt" | sha256sum | cut -d' ' -f1)
    
    if [ "$ORIGINAL_HASH" = "$DOWNLOADED_HASH" ]; then
        echo -e "${GREEN}✓ Large content verified after chunked upload${NC}"
    else
        echo -e "${RED}✗ Large content mismatch after chunked upload${NC}"
        echo "Original hash: $ORIGINAL_HASH"
        echo "Downloaded hash: $DOWNLOADED_HASH"
        exit 1
    fi
}

# Test 4: Upload using pipe (forces chunked transfer)
test_chunked_upload_pipe() {
    echo -e "\n${YELLOW}Test: Upload using pipe (forces chunked transfer)${NC}"
    
    # Create test content via pipe
    seq 1 1000 | aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp - s3://${CHUNKED_TEST_BUCKET}/chunked-pipe.txt
    
    # Verify the file was uploaded
    if aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 ls s3://${CHUNKED_TEST_BUCKET}/ | grep -q "chunked-pipe.txt"; then
        echo -e "${GREEN}✓ Piped chunked upload successful${NC}"
    else
        echo -e "${RED}✗ Piped chunked upload failed${NC}"
        exit 1
    fi
    
    # Download and verify content starts with "1" and ends with "1000"
    DOWNLOADED=$(aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp s3://${CHUNKED_TEST_BUCKET}/chunked-pipe.txt -)
    FIRST_LINE=$(echo "$DOWNLOADED" | head -1)
    LAST_LINE=$(echo "$DOWNLOADED" | tail -1)
    
    if [ "$FIRST_LINE" = "1" ] && [ "$LAST_LINE" = "1000" ]; then
        echo -e "${GREEN}✓ Piped content verified${NC}"
    else
        echo -e "${RED}✗ Piped content mismatch${NC}"
        echo "First line: $FIRST_LINE (expected: 1)"
        echo "Last line: $LAST_LINE (expected: 1000)"
        exit 1
    fi
}

# Test 5: Upload binary content via stdin
test_chunked_upload_binary() {
    echo -e "\n${YELLOW}Test: Upload binary content via stdin${NC}"
    
    # Create binary test content
    dd if=/dev/urandom of="${CHUNKED_TEST_DATA_DIR}/binary-original.bin" bs=1024 count=100 2>/dev/null
    ORIGINAL_HASH=$(sha256sum "${CHUNKED_TEST_DATA_DIR}/binary-original.bin" | cut -d' ' -f1)
    
    # Upload binary via stdin
    cat "${CHUNKED_TEST_DATA_DIR}/binary-original.bin" | aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp - s3://${CHUNKED_TEST_BUCKET}/chunked-binary.bin
    
    # Verify the file was uploaded
    if aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 ls s3://${CHUNKED_TEST_BUCKET}/ | grep -q "chunked-binary.bin"; then
        echo -e "${GREEN}✓ Binary chunked upload successful${NC}"
    else
        echo -e "${RED}✗ Binary chunked upload failed${NC}"
        exit 1
    fi
    
    # Download and verify binary content hash
    aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 cp s3://${CHUNKED_TEST_BUCKET}/chunked-binary.bin "${CHUNKED_TEST_DATA_DIR}/binary-downloaded.bin"
    DOWNLOADED_HASH=$(sha256sum "${CHUNKED_TEST_DATA_DIR}/binary-downloaded.bin" | cut -d' ' -f1)
    
    if [ "$ORIGINAL_HASH" = "$DOWNLOADED_HASH" ]; then
        echo -e "${GREEN}✓ Binary content verified after chunked upload${NC}"
    else
        echo -e "${RED}✗ Binary content mismatch after chunked upload${NC}"
        echo "Original hash: $ORIGINAL_HASH"
        echo "Downloaded hash: $DOWNLOADED_HASH"
        exit 1
    fi
}

# Test 6: Cleanup - delete all objects and bucket
test_chunked_cleanup() {
    echo -e "\n${YELLOW}Test: Cleanup chunked test objects and bucket${NC}"
    
    # Remove all objects
    aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 rm s3://${CHUNKED_TEST_BUCKET}/ --recursive
    
    # Delete the bucket
    aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 rb s3://${CHUNKED_TEST_BUCKET}
    
    # Verify bucket is deleted
    BUCKETS=$(aws --endpoint-url="${CHUNKED_SERVER_ADDR}" s3 ls)
    if ! echo "$BUCKETS" | grep -q "${CHUNKED_TEST_BUCKET}"; then
        echo -e "${GREEN}✓ Cleanup successful${NC}"
    else
        echo -e "${RED}✗ Bucket still exists${NC}"
        exit 1
    fi
}

# Main execution
main() {
    setup_chunked
    
    test_chunked_create_bucket
    test_chunked_upload_stdin
    test_chunked_upload_large_stdin
    test_chunked_upload_pipe
    test_chunked_upload_binary
    test_chunked_cleanup
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All chunked upload e2e tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main
fi
