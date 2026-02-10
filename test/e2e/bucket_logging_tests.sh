#!/bin/bash
# E2E tests for bucket logging with gzip-compressed access logs

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
export LOGGING_SERVER_PORT=9092
export LOGGING_SERVER_ADDR="http://localhost:${LOGGING_SERVER_PORT}"
export LOGGING_SOURCE_BUCKET="test-logging-source"
export LOGGING_TARGET_BUCKET="test-logging-target"
export LOGGING_TEST_DATA_DIR=$(mktemp -d)
export LOGGING_SERVER_DATA_DIR=$(mktemp -d)
export LOGGING_SERVER_PID=""

# Cleanup function
cleanup_logging() {
    echo -e "\n${YELLOW}Cleaning up logging tests...${NC}"
    if [ -n "$LOGGING_SERVER_PID" ]; then
        kill -TERM $LOGGING_SERVER_PID 2>/dev/null || true
        sleep 1
        kill -KILL $LOGGING_SERVER_PID 2>/dev/null || true
        wait $LOGGING_SERVER_PID 2>/dev/null || true
    fi
    rm -rf "${LOGGING_TEST_DATA_DIR}"
    rm -rf "${LOGGING_SERVER_DATA_DIR}"
}

# Setup function
setup_logging() {
    echo -e "${YELLOW}Starting S3-compatible server for logging tests...${NC}"
    echo "Server address: ${LOGGING_SERVER_ADDR}"
    echo "Test data directory: ${LOGGING_TEST_DATA_DIR}"
    echo "Server data directory: ${LOGGING_SERVER_DATA_DIR}"

    trap cleanup_logging EXIT

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

    # Start the server with a short log flush interval for testing
    echo -e "\n${YELLOW}Starting server...${NC}"
    ./s3d -addr ":${LOGGING_SERVER_PORT}" -data "${LOGGING_SERVER_DATA_DIR}" -log-flush-interval 1s > /dev/null 2>&1 &
    LOGGING_SERVER_PID=$!
    echo "Server PID: ${LOGGING_SERVER_PID}"

    # Wait for server to start
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "${LOGGING_SERVER_ADDR}" > /dev/null 2>&1; then
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
        exit 1
    fi

    # Configure AWS CLI with dummy credentials
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_DEFAULT_REGION=us-east-1
}

# Test 1: Create source and target buckets
test_logging_create_buckets() {
    echo -e "\n${YELLOW}Test: Create source and target buckets${NC}"
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 mb s3://${LOGGING_SOURCE_BUCKET}
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 mb s3://${LOGGING_TARGET_BUCKET}
    echo -e "${GREEN}✓ Buckets created${NC}"
}

# Test 2: Enable bucket logging
test_logging_enable() {
    echo -e "\n${YELLOW}Test: Enable bucket logging${NC}"

    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api put-bucket-logging \
        --bucket ${LOGGING_SOURCE_BUCKET} \
        --bucket-logging-status "{\"LoggingEnabled\":{\"TargetBucket\":\"${LOGGING_TARGET_BUCKET}\",\"TargetPrefix\":\"logs/\"}}"

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to enable bucket logging${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Bucket logging enabled${NC}"
}

# Test 3: Verify logging is enabled
test_logging_verify_enabled() {
    echo -e "\n${YELLOW}Test: Verify bucket logging is enabled${NC}"

    OUTPUT=$(aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api get-bucket-logging \
        --bucket ${LOGGING_SOURCE_BUCKET} 2>&1)

    if echo "$OUTPUT" | grep -q "${LOGGING_TARGET_BUCKET}"; then
        echo -e "${GREEN}✓ Bucket logging is enabled with correct target bucket${NC}"
    else
        echo -e "${RED}✗ Bucket logging not properly configured${NC}"
        echo "$OUTPUT"
        exit 1
    fi

    if echo "$OUTPUT" | grep -q "logs/"; then
        echo -e "${GREEN}✓ Target prefix is correct${NC}"
    else
        echo -e "${RED}✗ Target prefix not found${NC}"
        exit 1
    fi
}

# Test 4: Perform operations and verify gzip-compressed logs are written
test_logging_verify_gzip_logs() {
    echo -e "\n${YELLOW}Test: Perform operations and verify gzip-compressed access logs${NC}"

    # Upload a test object to the source bucket
    echo "Hello, access logging test!" > "${LOGGING_TEST_DATA_DIR}/test-object.txt"
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 cp \
        "${LOGGING_TEST_DATA_DIR}/test-object.txt" s3://${LOGGING_SOURCE_BUCKET}/test-object.txt

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to upload test object${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Test object uploaded${NC}"

    # Download the test object
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 cp \
        s3://${LOGGING_SOURCE_BUCKET}/test-object.txt "${LOGGING_TEST_DATA_DIR}/downloaded.txt"

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to download test object${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Test object downloaded${NC}"

    # Wait for the log flush interval to pass
    echo "  Waiting for log flush..."
    sleep 3

    # List log objects in the target bucket
    LOG_OBJECTS=$(aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${LOGGING_TARGET_BUCKET} \
        --prefix "logs/" 2>&1)

    if echo "$LOG_OBJECTS" | grep -q '"Key"'; then
        echo -e "${GREEN}✓ Access log objects found in target bucket${NC}"
    else
        echo -e "${RED}✗ No access log objects found${NC}"
        echo "$LOG_OBJECTS"
        exit 1
    fi

    # Verify log file has .gz extension
    if echo "$LOG_OBJECTS" | grep -q '\.gz"'; then
        echo -e "${GREEN}✓ Log file has .gz extension${NC}"
    else
        echo -e "${RED}✗ Log file does not have .gz extension${NC}"
        echo "$LOG_OBJECTS"
        exit 1
    fi

    # Get the first log object key
    LOG_KEY=$(echo "$LOG_OBJECTS" | python3 -c "import sys, json; contents = json.load(sys.stdin).get('Contents', []); print(contents[0]['Key'] if contents else '')" 2>/dev/null)

    if [ -z "$LOG_KEY" ]; then
        echo -e "${RED}✗ Could not extract log key${NC}"
        exit 1
    fi
    echo "  Log object key: $LOG_KEY"

    # Download the gzip-compressed log
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api get-object \
        --bucket ${LOGGING_TARGET_BUCKET} \
        --key "$LOG_KEY" \
        "${LOGGING_TEST_DATA_DIR}/access-log.gz" > /dev/null 2>&1

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to download access log${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Access log downloaded${NC}"

    # Verify the file is valid gzip
    if gzip -t "${LOGGING_TEST_DATA_DIR}/access-log.gz" 2>/dev/null; then
        echo -e "${GREEN}✓ Access log is valid gzip${NC}"
    else
        echo -e "${RED}✗ Access log is not valid gzip${NC}"
        exit 1
    fi

    # Decompress and verify content
    gzip -d -c "${LOGGING_TEST_DATA_DIR}/access-log.gz" > "${LOGGING_TEST_DATA_DIR}/access-log.txt"

    LOG_CONTENT=$(cat "${LOGGING_TEST_DATA_DIR}/access-log.txt")

    # Verify log contains expected source bucket name
    if echo "$LOG_CONTENT" | grep -q "${LOGGING_SOURCE_BUCKET}"; then
        echo -e "${GREEN}✓ Log contains source bucket name${NC}"
    else
        echo -e "${RED}✗ Log does not contain source bucket name${NC}"
        echo "  Log content: $LOG_CONTENT"
        exit 1
    fi

    # Verify log contains REST operations
    if echo "$LOG_CONTENT" | grep -q "REST\."; then
        echo -e "${GREEN}✓ Log contains REST operations${NC}"
    else
        echo -e "${RED}✗ Log does not contain REST operations${NC}"
        echo "  Log content: $LOG_CONTENT"
        exit 1
    fi

    # Verify log contains multiple lines (batched)
    LINE_COUNT=$(echo "$LOG_CONTENT" | grep -c "REST\." || true)
    if [ "$LINE_COUNT" -ge 2 ]; then
        echo -e "${GREEN}✓ Log contains $LINE_COUNT batched log lines${NC}"
    else
        echo -e "${YELLOW}! Log contains $LINE_COUNT line(s) (expected at least 2)${NC}"
    fi
}

# Test 5: Disable logging and verify no new logs
test_logging_disable() {
    echo -e "\n${YELLOW}Test: Disable logging and verify no new logs${NC}"

    # Count current log objects
    BEFORE_COUNT=$(aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${LOGGING_TARGET_BUCKET} --prefix "logs/" 2>&1 | \
        python3 -c "import sys, json; print(json.load(sys.stdin).get('KeyCount', 0))" 2>/dev/null)

    echo "  Log objects before disabling: $BEFORE_COUNT"

    # Disable logging
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api put-bucket-logging \
        --bucket ${LOGGING_SOURCE_BUCKET} \
        --bucket-logging-status "{}"

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to disable logging${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Logging disabled${NC}"

    # Verify logging is disabled
    LOGGING_OUTPUT=$(aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api get-bucket-logging \
        --bucket ${LOGGING_SOURCE_BUCKET} 2>&1)

    if echo "$LOGGING_OUTPUT" | grep -q "LoggingEnabled"; then
        echo -e "${RED}✗ Logging should be disabled${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Logging confirmed disabled${NC}"

    # Perform another operation
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${LOGGING_SOURCE_BUCKET} --key test-object.txt > /dev/null 2>&1

    # Wait for potential flush
    sleep 3

    # Count log objects after
    AFTER_COUNT=$(aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${LOGGING_TARGET_BUCKET} --prefix "logs/" 2>&1 | \
        python3 -c "import sys, json; print(json.load(sys.stdin).get('KeyCount', 0))" 2>/dev/null)

    echo "  Log objects after disabling: $AFTER_COUNT"

    if [ "$AFTER_COUNT" = "$BEFORE_COUNT" ]; then
        echo -e "${GREEN}✓ No new logs were written after disabling${NC}"
    else
        echo -e "${RED}✗ New logs were written after disabling ($BEFORE_COUNT -> $AFTER_COUNT)${NC}"
        exit 1
    fi
}

# Test 6: Cleanup
test_logging_cleanup() {
    echo -e "\n${YELLOW}Test: Cleanup logging test resources${NC}"

    # Delete objects
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 rm s3://${LOGGING_SOURCE_BUCKET}/ --recursive 2>/dev/null || true
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 rm s3://${LOGGING_TARGET_BUCKET}/ --recursive 2>/dev/null || true

    # Delete buckets
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 rb s3://${LOGGING_SOURCE_BUCKET} 2>/dev/null || true
    aws --endpoint-url="${LOGGING_SERVER_ADDR}" --no-sign-request s3 rb s3://${LOGGING_TARGET_BUCKET} 2>/dev/null || true

    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Main execution
main() {
    setup_logging

    test_logging_create_buckets
    test_logging_enable
    test_logging_verify_enabled
    test_logging_verify_gzip_logs
    test_logging_disable
    test_logging_cleanup

    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All bucket logging e2e tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main
fi
