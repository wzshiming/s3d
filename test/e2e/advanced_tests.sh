#!/bin/bash
# Tests for multipart upload and advanced features

set -e

# Source common utilities if not already sourced
if [ -z "$SERVER_ADDR" ]; then
    source "$(dirname "$0")/common.sh"
fi

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

# Test 13: UploadPartCopy - Copy existing object to multipart upload part
test_upload_part_copy() {
    echo -e "\n${YELLOW}Test: UploadPartCopy (copy object to multipart part)${NC}"
    
    # Create bucket if it doesn't exist
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET} 2>/dev/null || true
    
    # Create a source object to copy from
    echo "Source object content for UploadPartCopy test" > "${TEST_DATA_DIR}/source-for-copy.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/source-for-copy.txt" s3://${TEST_BUCKET}/source-for-copy.txt
    
    # Create part 1 data
    echo "Part 1 data" > "${TEST_DATA_DIR}/part1.txt"
    
    # Initiate multipart upload
    UPLOAD_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api create-multipart-upload \
        --bucket ${TEST_BUCKET} \
        --key multipart-with-copy.txt 2>&1)
    UPLOAD_ID=$(echo "$UPLOAD_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin)['UploadId'])" 2>/dev/null || echo "")
    
    if [ -z "$UPLOAD_ID" ]; then
        echo -e "${RED}✗ Failed to initiate multipart upload${NC}"
        exit 1
    fi
    
    echo "Upload ID: $UPLOAD_ID"
    
    # Upload part 1 from local file
    PART1_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api upload-part \
        --bucket ${TEST_BUCKET} \
        --key multipart-with-copy.txt \
        --part-number 1 \
        --upload-id "$UPLOAD_ID" \
        --body "${TEST_DATA_DIR}/part1.txt" 2>&1)
    PART1_ETAG=$(echo "$PART1_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin)['ETag'])" 2>/dev/null || echo "")
    
    echo "Part 1 ETag: $PART1_ETAG"
    
    # Upload part 2 using UploadPartCopy from the source object
    PART2_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api upload-part-copy \
        --bucket ${TEST_BUCKET} \
        --key multipart-with-copy.txt \
        --part-number 2 \
        --upload-id "$UPLOAD_ID" \
        --copy-source "${TEST_BUCKET}/source-for-copy.txt" 2>&1)
    PART2_ETAG=$(echo "$PART2_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin)['CopyPartResult']['ETag'])" 2>/dev/null || echo "")
    
    if [ -z "$PART2_ETAG" ]; then
        echo -e "${RED}✗ UploadPartCopy failed${NC}"
        # Abort the upload
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api abort-multipart-upload \
            --bucket ${TEST_BUCKET} \
            --key multipart-with-copy.txt \
            --upload-id "$UPLOAD_ID" 2>/dev/null || true
        exit 1
    fi
    
    echo "Part 2 ETag (from UploadPartCopy): $PART2_ETAG"
    
    # Complete multipart upload with proper JSON formatting
    cat > "${TEST_DATA_DIR}/parts.json" <<EOF
{
    "Parts": [
        {
            "ETag": $PART1_ETAG,
            "PartNumber": 1
        },
        {
            "ETag": $PART2_ETAG,
            "PartNumber": 2
        }
    ]
}
EOF
    
    COMPLETE_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api complete-multipart-upload \
        --bucket ${TEST_BUCKET} \
        --key multipart-with-copy.txt \
        --upload-id "$UPLOAD_ID" \
        --multipart-upload "file://${TEST_DATA_DIR}/parts.json" 2>&1)
    
    # Verify the object exists
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "multipart-with-copy.txt"; then
        echo -e "${GREEN}✓ UploadPartCopy completed successfully${NC}"
    else
        echo -e "${RED}✗ Multipart upload with copy failed${NC}"
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

# Run tests if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    setup
    
    # Create bucket for tests
    test_create_bucket() {
        echo -e "\n${YELLOW}Test: Create bucket for advanced tests${NC}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET}
        echo -e "${GREEN}✓ Bucket created${NC}"
    }
    
    test_create_bucket
    test_multipart_upload
    test_sync_directory
    test_upload_part_copy
    test_nested_paths
    
    # Cleanup
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${TEST_BUCKET} --force
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Advanced tests passed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
fi
