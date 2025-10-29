#!/bin/bash
# E2E tests for bucket logging operations

source "$(dirname "$0")/common.sh"

# Test bucket names
SOURCE_BUCKET="test-source-bucket-logging"
TARGET_BUCKET="test-target-bucket-logging"

# Test 1: Get bucket logging (should be empty initially)
test_get_bucket_logging_empty() {
    echo -e "\n${YELLOW}Test: Get bucket logging (should be empty)${NC}"
    
    # Create source bucket first
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${SOURCE_BUCKET}
    
    # Get logging configuration (should be empty)
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-bucket-logging --bucket ${SOURCE_BUCKET} 2>&1)
    
    # Check if LoggingEnabled is not present in output
    if echo "$RESULT" | grep -q "LoggingEnabled"; then
        echo -e "${RED}✗ Expected logging to be disabled by default${NC}"
        echo "$RESULT"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Logging is disabled by default${NC}"
}

# Test 2: Enable bucket logging
test_enable_bucket_logging() {
    echo -e "\n${YELLOW}Test: Enable bucket logging${NC}"
    
    # Create target bucket for logs
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TARGET_BUCKET}
    
    # Enable logging
    cat > /tmp/logging-config.json << EOF
{
  "LoggingEnabled": {
    "TargetBucket": "${TARGET_BUCKET}",
    "TargetPrefix": "access-logs/"
  }
}
EOF
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-bucket-logging \
        --bucket ${SOURCE_BUCKET} \
        --bucket-logging-status file:///tmp/logging-config.json
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Bucket logging enabled${NC}"
    else
        echo -e "${RED}✗ Failed to enable bucket logging${NC}"
        exit 1
    fi
    
    rm -f /tmp/logging-config.json
}

# Test 3: Get bucket logging (should be enabled)
test_get_bucket_logging_enabled() {
    echo -e "\n${YELLOW}Test: Get bucket logging (should be enabled)${NC}"
    
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-bucket-logging --bucket ${SOURCE_BUCKET})
    
    # Check if LoggingEnabled is present
    if ! echo "$RESULT" | grep -q "LoggingEnabled"; then
        echo -e "${RED}✗ Expected logging to be enabled${NC}"
        echo "$RESULT"
        exit 1
    fi
    
    # Check target bucket
    if ! echo "$RESULT" | grep -q "\"TargetBucket\": \"${TARGET_BUCKET}\""; then
        echo -e "${RED}✗ Target bucket mismatch${NC}"
        echo "$RESULT"
        exit 1
    fi
    
    # Check target prefix
    if ! echo "$RESULT" | grep -q "\"TargetPrefix\": \"access-logs/\""; then
        echo -e "${RED}✗ Target prefix mismatch${NC}"
        echo "$RESULT"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Logging configuration verified${NC}"
}

# Test 4: Update bucket logging (change prefix)
test_update_bucket_logging() {
    echo -e "\n${YELLOW}Test: Update bucket logging configuration${NC}"
    
    # Update logging configuration
    cat > /tmp/logging-config-updated.json << EOF
{
  "LoggingEnabled": {
    "TargetBucket": "${TARGET_BUCKET}",
    "TargetPrefix": "logs/updated/"
  }
}
EOF
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-bucket-logging \
        --bucket ${SOURCE_BUCKET} \
        --bucket-logging-status file:///tmp/logging-config-updated.json
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to update bucket logging${NC}"
        exit 1
    fi
    
    # Verify update
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-bucket-logging --bucket ${SOURCE_BUCKET})
    
    if ! echo "$RESULT" | grep -q "\"TargetPrefix\": \"logs/updated/\""; then
        echo -e "${RED}✗ Logging configuration was not updated${NC}"
        echo "$RESULT"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Logging configuration updated${NC}"
    
    rm -f /tmp/logging-config-updated.json
}

# Test 5: Generate access logs
test_generate_access_logs() {
    echo -e "\n${YELLOW}Test: Generate access logs${NC}"
    
    # Perform some operations to generate logs
    echo "Test content" > "${TEST_DATA_DIR}/test-file-1.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/test-file-1.txt" s3://${SOURCE_BUCKET}/test-file-1.txt
    
    echo "Test content 2" > "${TEST_DATA_DIR}/test-file-2.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/test-file-2.txt" s3://${SOURCE_BUCKET}/test-file-2.txt
    
    # Get the file
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        s3://${SOURCE_BUCKET}/test-file-1.txt "${TEST_DATA_DIR}/downloaded.txt" > /dev/null 2>&1
    
    # List objects
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${SOURCE_BUCKET}/ > /dev/null 2>&1
    
    echo -e "${GREEN}✓ Operations performed (logs will be generated)${NC}"
    echo -e "${YELLOW}Note: Logs are batched and flushed periodically (every hour or 100 entries)${NC}"
}

# Test 6: Disable bucket logging
test_disable_bucket_logging() {
    echo -e "\n${YELLOW}Test: Disable bucket logging${NC}"
    
    # Disable logging (empty configuration)
    cat > /tmp/logging-config-disable.json << EOF
{}
EOF
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-bucket-logging \
        --bucket ${SOURCE_BUCKET} \
        --bucket-logging-status file:///tmp/logging-config-disable.json
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to disable bucket logging${NC}"
        exit 1
    fi
    
    # Verify logging is disabled
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-bucket-logging --bucket ${SOURCE_BUCKET})
    
    if echo "$RESULT" | grep -q "LoggingEnabled"; then
        echo -e "${RED}✗ Expected logging to be disabled${NC}"
        echo "$RESULT"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Bucket logging disabled${NC}"
    
    rm -f /tmp/logging-config-disable.json
}

# Test 7: Test logging with non-existent bucket
test_logging_nonexistent_bucket() {
    echo -e "\n${YELLOW}Test: Get/Put logging for non-existent bucket${NC}"
    
    # Try to get logging for non-existent bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-bucket-logging \
        --bucket non-existent-bucket-xyz > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo -e "${RED}✗ Expected error for non-existent bucket${NC}"
        exit 1
    fi
    
    # Try to put logging for non-existent bucket
    cat > /tmp/logging-config-test.json << EOF
{
  "LoggingEnabled": {
    "TargetBucket": "${TARGET_BUCKET}",
    "TargetPrefix": "test/"
  }
}
EOF
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-bucket-logging \
        --bucket non-existent-bucket-xyz \
        --bucket-logging-status file:///tmp/logging-config-test.json > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo -e "${RED}✗ Expected error for non-existent bucket${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Correctly handles non-existent bucket${NC}"
    
    rm -f /tmp/logging-config-test.json
}

# Cleanup test buckets
cleanup_logging_tests() {
    echo -e "\n${YELLOW}Cleaning up logging test buckets...${NC}"
    
    # Delete objects from source bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${SOURCE_BUCKET}/ --recursive > /dev/null 2>&1
    
    # Delete objects from target bucket (logs)
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TARGET_BUCKET}/ --recursive > /dev/null 2>&1
    
    # Delete buckets
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${SOURCE_BUCKET} > /dev/null 2>&1
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${TARGET_BUCKET} > /dev/null 2>&1
    
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Main test execution
run_logging_tests() {
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Running Bucket Logging E2E Tests${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    test_get_bucket_logging_empty
    test_enable_bucket_logging
    test_get_bucket_logging_enabled
    test_update_bucket_logging
    test_generate_access_logs
    test_disable_bucket_logging
    test_logging_nonexistent_bucket
    
    cleanup_logging_tests
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All Bucket Logging Tests Passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Run tests if script is executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    setup
    run_logging_tests
fi
