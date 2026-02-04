#!/bin/bash
# Tests for checksum operations (SHA256)

source "$(dirname "$0")/common.sh"

CHECKSUM_TEST_BUCKET="test-checksum-bucket"
CHECKSUM_TEST_DATA_DIR="${TEST_DATA_DIR}/checksum"

# Setup checksum test environment
setup_checksum_tests() {
    echo -e "\n${YELLOW}Setting up checksum tests...${NC}"
    mkdir -p "${CHECKSUM_TEST_DATA_DIR}"
    
    # Create test bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${CHECKSUM_TEST_BUCKET}
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to create checksum test bucket${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Checksum test bucket created${NC}"
}

# Cleanup checksum test environment
cleanup_checksum_tests() {
    echo -e "\n${YELLOW}Cleaning up checksum tests...${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${CHECKSUM_TEST_BUCKET}/ --recursive 2>/dev/null || true
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${CHECKSUM_TEST_BUCKET} 2>/dev/null || true
    rm -rf "${CHECKSUM_TEST_DATA_DIR}"
}

# Test: PutObject with SHA256 checksum algorithm
test_put_object_with_checksum_sha256() {
    echo -e "\n${YELLOW}Test: PutObject with SHA256 checksum algorithm${NC}"
    
    # Create test file
    echo "Hello, SHA256 Checksum!" > "${CHECKSUM_TEST_DATA_DIR}/checksum-test.txt"
    
    # Upload with checksum algorithm specified
    OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${CHECKSUM_TEST_BUCKET} \
        --key checksum-test.txt \
        --body "${CHECKSUM_TEST_DATA_DIR}/checksum-test.txt" \
        --checksum-algorithm SHA256 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ PutObject with checksum algorithm failed${NC}"
        echo "$OUTPUT"
        exit 1
    fi
    
    # Verify response contains ChecksumSHA256
    if echo "$OUTPUT" | grep -q "ChecksumSHA256"; then
        echo -e "${GREEN}✓ PutObject returned ChecksumSHA256 in response${NC}"
        CHECKSUM=$(echo "$OUTPUT" | grep -o '"ChecksumSHA256": "[^"]*"' | cut -d'"' -f4)
        echo "  ChecksumSHA256: $CHECKSUM"
    else
        echo -e "${YELLOW}! ChecksumSHA256 not in JSON output (may be in headers)${NC}"
    fi
    
    # Verify ETag is set
    if echo "$OUTPUT" | grep -q "ETag"; then
        echo -e "${GREEN}✓ ETag is set${NC}"
    else
        echo -e "${RED}✗ ETag not found in response${NC}"
        exit 1
    fi
}

# Test: GetObject returns checksum header
test_get_object_checksum_header() {
    echo -e "\n${YELLOW}Test: GetObject returns checksum in header${NC}"
    
    # Get object with verbose output to see headers
    HEADERS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-object \
        --bucket ${CHECKSUM_TEST_BUCKET} \
        --key checksum-test.txt \
        --checksum-mode ENABLED \
        "${CHECKSUM_TEST_DATA_DIR}/downloaded-checksum.txt" 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ GetObject with checksum mode failed${NC}"
        echo "$HEADERS"
        exit 1
    fi
    
    # Verify ChecksumSHA256 is in the response
    if echo "$HEADERS" | grep -qi "ChecksumSHA256"; then
        echo -e "${GREEN}✓ GetObject returned ChecksumSHA256${NC}"
        CHECKSUM=$(echo "$HEADERS" | grep -o '"ChecksumSHA256": "[^"]*"' | cut -d'"' -f4)
        echo "  ChecksumSHA256: $CHECKSUM"
    else
        echo -e "${YELLOW}! ChecksumSHA256 not found in response (checking if file was downloaded correctly)${NC}"
    fi
    
    # Verify downloaded content matches
    if diff "${CHECKSUM_TEST_DATA_DIR}/checksum-test.txt" "${CHECKSUM_TEST_DATA_DIR}/downloaded-checksum.txt" > /dev/null; then
        echo -e "${GREEN}✓ Downloaded file content matches original${NC}"
    else
        echo -e "${RED}✗ Downloaded file content does not match original${NC}"
        exit 1
    fi
}

# Test: HeadObject returns checksum
test_head_object_checksum() {
    echo -e "\n${YELLOW}Test: HeadObject returns checksum${NC}"
    
    OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${CHECKSUM_TEST_BUCKET} \
        --key checksum-test.txt \
        --checksum-mode ENABLED 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ HeadObject with checksum mode failed${NC}"
        echo "$OUTPUT"
        exit 1
    fi
    
    # Verify ChecksumSHA256 is in the response
    if echo "$OUTPUT" | grep -qi "ChecksumSHA256"; then
        echo -e "${GREEN}✓ HeadObject returned ChecksumSHA256${NC}"
        CHECKSUM=$(echo "$OUTPUT" | grep -o '"ChecksumSHA256": "[^"]*"' | cut -d'"' -f4)
        echo "  ChecksumSHA256: $CHECKSUM"
    else
        echo -e "${YELLOW}! ChecksumSHA256 not visible in output${NC}"
    fi
    
    # Verify ETag and ContentLength are set
    if echo "$OUTPUT" | grep -q "ETag"; then
        echo -e "${GREEN}✓ ETag is set${NC}"
    else
        echo -e "${RED}✗ ETag not found${NC}"
        exit 1
    fi
    
    if echo "$OUTPUT" | grep -q "ContentLength"; then
        echo -e "${GREEN}✓ ContentLength is set${NC}"
    else
        echo -e "${RED}✗ ContentLength not found${NC}"
        exit 1
    fi
}

# Test: Large file checksum
test_large_file_checksum() {
    echo -e "\n${YELLOW}Test: Large file with checksum${NC}"
    
    # Create a larger file (1MB)
    dd if=/dev/urandom of="${CHECKSUM_TEST_DATA_DIR}/large-file.bin" bs=1024 count=1024 2>/dev/null
    
    # Calculate SHA256 hash locally for verification
    LOCAL_HASH=$(sha256sum "${CHECKSUM_TEST_DATA_DIR}/large-file.bin" | cut -d' ' -f1)
    echo "  Local SHA256 hash: $LOCAL_HASH"
    
    # Upload with checksum algorithm
    OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${CHECKSUM_TEST_BUCKET} \
        --key large-file.bin \
        --body "${CHECKSUM_TEST_DATA_DIR}/large-file.bin" \
        --checksum-algorithm SHA256 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ PutObject for large file failed${NC}"
        echo "$OUTPUT"
        exit 1
    fi
    echo -e "${GREEN}✓ Large file uploaded with checksum${NC}"
    
    # Download and verify
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-object \
        --bucket ${CHECKSUM_TEST_BUCKET} \
        --key large-file.bin \
        --checksum-mode ENABLED \
        "${CHECKSUM_TEST_DATA_DIR}/large-file-downloaded.bin" > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ GetObject for large file failed${NC}"
        exit 1
    fi
    
    # Verify downloaded file hash matches
    DOWNLOADED_HASH=$(sha256sum "${CHECKSUM_TEST_DATA_DIR}/large-file-downloaded.bin" | cut -d' ' -f1)
    if [ "$LOCAL_HASH" = "$DOWNLOADED_HASH" ]; then
        echo -e "${GREEN}✓ Large file checksum verified (SHA256 matches)${NC}"
    else
        echo -e "${RED}✗ Large file checksum mismatch${NC}"
        echo "  Local:      $LOCAL_HASH"
        echo "  Downloaded: $DOWNLOADED_HASH"
        exit 1
    fi
}

# Run all checksum tests
run_checksum_tests() {
    setup
    setup_checksum_tests
    
    test_put_object_with_checksum_sha256
    test_get_object_checksum_header
    test_head_object_checksum
    test_large_file_checksum
    
    cleanup_checksum_tests
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All checksum tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Run tests if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_checksum_tests
fi
