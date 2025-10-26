#!/bin/bash
# Tests for multipart upload and advanced features

source "$(dirname "$0")/common.sh"

# Test 11: Upload large file (multipart)
test_multipart_upload() {
    echo -e "\n${YELLOW}Test: Upload large file (multipart)${NC}"
    # Create a 10MB file
    dd if=/dev/zero of="${TEST_DATA_DIR}/large-file.bin" bs=1M count=10 2>/dev/null
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/large-file.bin" s3://${TEST_BUCKET}/large-file.bin
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "large-file.bin"; then
        echo -e "${GREEN}✓ Large file uploaded${NC}"
    else
        echo -e "${RED}✗ Large file not found${NC}"
        exit 1
    fi
}

# Test 12: Sync directory
test_sync_directory() {
    echo -e "\n${YELLOW}Test: Sync directory${NC}"
    mkdir -p "${TEST_DATA_DIR}/sync-test"
    for i in {1..3}; do
        echo "Sync test $i" > "${TEST_DATA_DIR}/sync-test/sync-${i}.txt"
    done
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 sync "${TEST_DATA_DIR}/sync-test/" s3://${TEST_BUCKET}/synced/
    SYNCED_COUNT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/synced/ | grep -c "sync-" || true)
    if [ "$SYNCED_COUNT" -eq 3 ]; then
        echo -e "${GREEN}✓ Directory synced successfully${NC}"
    else
        echo -e "${RED}✗ Expected 3 synced files, found ${SYNCED_COUNT}${NC}"
        exit 1
    fi
}

# Test 15: Test with nested paths
test_nested_paths() {
    echo -e "\n${YELLOW}Test: Test with nested paths${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET} 2>/dev/null || true
    echo "Nested content" > "${TEST_DATA_DIR}/nested.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/nested.txt" s3://${TEST_BUCKET}/level1/level2/level3/nested.txt
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/level1/level2/level3/ | grep -q "nested.txt"; then
        echo -e "${GREEN}✓ Nested path upload successful${NC}"
    else
        echo -e "${RED}✗ Nested path upload failed${NC}"
        exit 1
    fi
}
