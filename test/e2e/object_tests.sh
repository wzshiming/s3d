#!/bin/bash
# Tests for object operations

source "$(dirname "$0")/common.sh"

# Test 4: Upload a file
test_upload_file() {
    echo -e "\n${YELLOW}Test: Upload a file${NC}"
    echo "Hello, S3!" > "${TEST_DATA_DIR}/test-file.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/test-file.txt" s3://${TEST_BUCKET}/test-file.txt
    echo -e "${GREEN}✓ File uploaded${NC}"
}

# Test 5: List objects
test_list_objects() {
    echo -e "\n${YELLOW}Test: List objects${NC}"
    OBJECTS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/)
    if echo "$OBJECTS" | grep -q "test-file.txt"; then
        echo -e "${GREEN}✓ Object listed successfully${NC}"
    else
        echo -e "${RED}✗ Object not found in list${NC}"
        exit 1
    fi
}

# Test 6: Download the file
test_download_file() {
    echo -e "\n${YELLOW}Test: Download the file${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/test-file.txt "${TEST_DATA_DIR}/downloaded.txt"
    if diff "${TEST_DATA_DIR}/test-file.txt" "${TEST_DATA_DIR}/downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ File downloaded and verified${NC}"
    else
        echo -e "${RED}✗ Downloaded file does not match original${NC}"
        exit 1
    fi
}

# Test 7: Upload multiple files
test_upload_multiple_files() {
    echo -e "\n${YELLOW}Test: Upload multiple files${NC}"
    for i in {1..5}; do
        echo "Test content $i" > "${TEST_DATA_DIR}/file-${i}.txt"
    done
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/" s3://${TEST_BUCKET}/files/ --recursive
    echo -e "${GREEN}✓ Multiple files uploaded${NC}"
}

# Test 8: List objects with prefix
test_list_with_prefix() {
    echo -e "\n${YELLOW}Test: List objects with prefix${NC}"
    OBJECTS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/files/)
    FILE_COUNT=$(echo "$OBJECTS" | grep -c "file-" || true)
    if [ "$FILE_COUNT" -eq 5 ]; then
        echo -e "${GREEN}✓ All files listed with prefix${NC}"
    else
        echo -e "${RED}✗ Expected 5 files, found ${FILE_COUNT}${NC}"
        exit 1
    fi
}

# Test 9: Copy object
test_copy_object() {
    echo -e "\n${YELLOW}Test: Copy object${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/test-file.txt s3://${TEST_BUCKET}/test-file-copy.txt
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "test-file-copy.txt"; then
        echo -e "${GREEN}✓ Object copied successfully${NC}"
    else
        echo -e "${RED}✗ Copied object not found${NC}"
        exit 1
    fi
}

# Test 10: Delete object
test_delete_object() {
    echo -e "\n${YELLOW}Test: Delete object${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/test-file-copy.txt
    if ! aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "test-file-copy.txt"; then
        echo -e "${GREEN}✓ Object deleted successfully${NC}"
    else
        echo -e "${RED}✗ Object still exists after deletion${NC}"
        exit 1
    fi
}

# Test 11: Rename object
test_rename_object() {
    echo -e "\n${YELLOW}Test: Rename object${NC}"
    # Upload a test file for renaming
    echo "Content for rename test" > "${TEST_DATA_DIR}/rename-test.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "${TEST_DATA_DIR}/rename-test.txt" s3://${TEST_BUCKET}/rename-test.txt
    
    # Rename the object
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api rename-object \
        --bucket ${TEST_BUCKET} \
        --key renamed-test.txt \
        --rename-source ${TEST_BUCKET}/rename-test.txt
    
    # Verify renamed object exists
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "renamed-test.txt"; then
        echo -e "${GREEN}✓ Object renamed successfully${NC}"
    else
        echo -e "${RED}✗ Renamed object not found${NC}"
        exit 1
    fi
    
    # Verify original object no longer exists
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "rename-test.txt"; then
        echo -e "${RED}✗ Original object still exists after rename${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ Original object removed after rename${NC}"
    fi
    
    # Verify content is preserved
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp s3://${TEST_BUCKET}/renamed-test.txt "${TEST_DATA_DIR}/renamed-downloaded.txt"
    if diff "${TEST_DATA_DIR}/rename-test.txt" "${TEST_DATA_DIR}/renamed-downloaded.txt" > /dev/null; then
        echo -e "${GREEN}✓ Renamed object content verified${NC}"
    else
        echo -e "${RED}✗ Renamed object content does not match original${NC}"
        exit 1
    fi
    
    # Clean up
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/renamed-test.txt
}

# Test 13: Remove all objects
test_remove_all_objects() {
    echo -e "\n${YELLOW}Test: Remove all objects${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/ --recursive
    REMAINING=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | wc -l)
    if [ "$REMAINING" -eq 0 ]; then
        echo -e "${GREEN}✓ All objects removed${NC}"
    else
        echo -e "${RED}✗ ${REMAINING} objects still remain${NC}"
        exit 1
    fi
}
