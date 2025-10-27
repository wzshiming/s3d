#!/bin/bash
# Tests for duplicate write compatibility

source "$(dirname "$0")/common.sh"

# Test: PutObject with same content twice
test_put_duplicate_same_content() {
    echo -e "\n${YELLOW}Test: PutObject with duplicate same content${NC}"
    
    # Create test file
    echo "Duplicate content test" > "${TEST_DATA_DIR}/duplicate-test.txt"
    
    # First upload
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/duplicate-test.txt" s3://${TEST_BUCKET}/duplicate-test.txt
    ETAG1=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object --bucket ${TEST_BUCKET} --key duplicate-test.txt --query 'ETag' --output text 2>/dev/null)
    
    # Second upload with same content
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/duplicate-test.txt" s3://${TEST_BUCKET}/duplicate-test.txt
    ETAG2=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object --bucket ${TEST_BUCKET} --key duplicate-test.txt --query 'ETag' --output text 2>/dev/null)
    
    # ETags should be the same
    if [ "$ETAG1" = "$ETAG2" ]; then
        echo -e "${GREEN}✓ ETags match for duplicate content (${ETAG1})${NC}"
    else
        echo -e "${RED}✗ ETags differ: ${ETAG1} vs ${ETAG2}${NC}"
        exit 1
    fi
    
    # Verify content is correct
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/duplicate-test.txt "${TEST_DATA_DIR}/duplicate-downloaded.txt"
    if diff "${TEST_DATA_DIR}/duplicate-test.txt" "${TEST_DATA_DIR}/duplicate-downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ Content verified after duplicate put${NC}"
    else
        echo -e "${RED}✗ Content mismatch after duplicate put${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/duplicate-test.txt
}

# Test: PutObject with different content (overwrite)
test_put_duplicate_different_content() {
    echo -e "\n${YELLOW}Test: PutObject with duplicate different content (overwrite)${NC}"
    
    # Create first version
    echo "First version" > "${TEST_DATA_DIR}/overwrite-test.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/overwrite-test.txt" s3://${TEST_BUCKET}/overwrite-test.txt
    ETAG1=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object --bucket ${TEST_BUCKET} --key overwrite-test.txt --query 'ETag' --output text 2>/dev/null)
    
    # Create second version with different content
    echo "Second different version" > "${TEST_DATA_DIR}/overwrite-test.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/overwrite-test.txt" s3://${TEST_BUCKET}/overwrite-test.txt
    ETAG2=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object --bucket ${TEST_BUCKET} --key overwrite-test.txt --query 'ETag' --output text 2>/dev/null)
    
    # ETags should be different
    if [ "$ETAG1" != "$ETAG2" ]; then
        echo -e "${GREEN}✓ ETags differ for different content${NC}"
    else
        echo -e "${RED}✗ ETags should differ for different content${NC}"
        exit 1
    fi
    
    # Verify content is the new version
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/overwrite-test.txt "${TEST_DATA_DIR}/overwrite-downloaded.txt"
    DOWNLOADED_CONTENT=$(cat "${TEST_DATA_DIR}/overwrite-downloaded.txt")
    if [ "$DOWNLOADED_CONTENT" = "Second different version" ]; then
        echo -e "${GREEN}✓ Content correctly overwritten${NC}"
    else
        echo -e "${RED}✗ Content not overwritten correctly: ${DOWNLOADED_CONTENT}${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/overwrite-test.txt
}

# Test: CopyObject to existing destination with same content
test_copy_to_existing_same_content() {
    echo -e "\n${YELLOW}Test: CopyObject to existing destination with same content${NC}"
    
    # Create source and destination with same content
    echo "Shared content" > "${TEST_DATA_DIR}/copy-shared.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/copy-shared.txt" s3://${TEST_BUCKET}/copy-source-same.txt
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/copy-shared.txt" s3://${TEST_BUCKET}/copy-dest-same.txt
    
    # Copy source to destination (both have same content)
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/copy-source-same.txt s3://${TEST_BUCKET}/copy-dest-same.txt
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Copy to existing destination with same content succeeded${NC}"
    else
        echo -e "${RED}✗ Copy to existing destination with same content failed${NC}"
        exit 1
    fi
    
    # Verify destination still has correct content
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/copy-dest-same.txt "${TEST_DATA_DIR}/copy-dest-same-downloaded.txt"
    if diff "${TEST_DATA_DIR}/copy-shared.txt" "${TEST_DATA_DIR}/copy-dest-same-downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ Destination content verified${NC}"
    else
        echo -e "${RED}✗ Destination content mismatch${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/copy-source-same.txt
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/copy-dest-same.txt
}

# Test: CopyObject to existing destination with different content
test_copy_to_existing_different_content() {
    echo -e "\n${YELLOW}Test: CopyObject to existing destination with different content${NC}"
    
    # Create source and destination with different content
    echo "Source content" > "${TEST_DATA_DIR}/copy-source-diff.txt"
    echo "Original destination content" > "${TEST_DATA_DIR}/copy-dest-diff.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/copy-source-diff.txt" s3://${TEST_BUCKET}/copy-source-diff.txt
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/copy-dest-diff.txt" s3://${TEST_BUCKET}/copy-dest-diff.txt
    
    # Copy source to destination (overwrite)
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/copy-source-diff.txt s3://${TEST_BUCKET}/copy-dest-diff.txt
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Copy to existing destination with different content succeeded${NC}"
    else
        echo -e "${RED}✗ Copy failed${NC}"
        exit 1
    fi
    
    # Verify destination has source content now
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/copy-dest-diff.txt "${TEST_DATA_DIR}/copy-dest-diff-downloaded.txt"
    if diff "${TEST_DATA_DIR}/copy-source-diff.txt" "${TEST_DATA_DIR}/copy-dest-diff-downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ Destination correctly overwritten with source content${NC}"
    else
        echo -e "${RED}✗ Destination content mismatch${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/copy-source-diff.txt
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/copy-dest-diff.txt
}

# Test: RenameObject to existing destination with same content
test_rename_to_existing_same_content() {
    echo -e "\n${YELLOW}Test: RenameObject to existing destination with same content${NC}"
    
    # Create source and destination with same content
    echo "Same content for rename" > "${TEST_DATA_DIR}/rename-same.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/rename-same.txt" s3://${TEST_BUCKET}/rename-source-same.txt
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/rename-same.txt" s3://${TEST_BUCKET}/rename-dest-same.txt
    
    # Rename source to destination (both have same content)
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api rename-object \
        --bucket ${TEST_BUCKET} \
        --key rename-dest-same.txt \
        --rename-source ${TEST_BUCKET}/rename-source-same.txt
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Rename to existing destination with same content succeeded${NC}"
    else
        echo -e "${RED}✗ Rename to existing destination with same content failed${NC}"
        exit 1
    fi
    
    # Source should be deleted
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "rename-source-same.txt"; then
        echo -e "${RED}✗ Source still exists after rename${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ Source deleted after rename${NC}"
    fi
    
    # Destination should still exist with correct content
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/rename-dest-same.txt "${TEST_DATA_DIR}/rename-dest-same-downloaded.txt"
    if diff "${TEST_DATA_DIR}/rename-same.txt" "${TEST_DATA_DIR}/rename-dest-same-downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ Destination content verified${NC}"
    else
        echo -e "${RED}✗ Destination content mismatch${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/rename-dest-same.txt
}

# Test: RenameObject to existing destination with different content (should overwrite)
test_rename_to_existing_different_content() {
    echo -e "\n${YELLOW}Test: RenameObject to existing destination with different content (should overwrite)${NC}"
    
    # Create source and destination with different content
    echo "Source content for rename" > "${TEST_DATA_DIR}/rename-source-diff.txt"
    echo "Destination content for rename" > "${TEST_DATA_DIR}/rename-dest-diff.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/rename-source-diff.txt" s3://${TEST_BUCKET}/rename-source-diff.txt
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/rename-dest-diff.txt" s3://${TEST_BUCKET}/rename-dest-diff.txt
    
    # Rename should succeed and overwrite destination
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api rename-object \
        --bucket ${TEST_BUCKET} \
        --key rename-dest-diff.txt \
        --rename-source ${TEST_BUCKET}/rename-source-diff.txt
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Rename to existing destination with different content succeeded${NC}"
    else
        echo -e "${RED}✗ Rename should have succeeded${NC}"
        exit 1
    fi
    
    # Source should be deleted (rename succeeded)
    if ! aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "rename-source-diff.txt"; then
        echo -e "${GREEN}✓ Source deleted after rename${NC}"
    else
        echo -e "${RED}✗ Source still exists after rename${NC}"
        exit 1
    fi
    
    # Destination should now have source content (overwritten)
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/rename-dest-diff.txt "${TEST_DATA_DIR}/rename-dest-diff-downloaded.txt"
    DOWNLOADED_CONTENT=$(cat "${TEST_DATA_DIR}/rename-dest-diff-downloaded.txt")
    if [ "$DOWNLOADED_CONTENT" = "Source content for rename" ]; then
        echo -e "${GREEN}✓ Destination has source content after rename${NC}"
    else
        echo -e "${RED}✗ Destination content incorrect: ${DOWNLOADED_CONTENT}${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/rename-dest-diff.txt
}
