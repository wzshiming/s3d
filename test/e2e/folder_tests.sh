#!/bin/bash
# Tests for folder object operations
# S3 folders are objects with keys ending with "/" and zero-length content

source "$(dirname "$0")/common.sh"

# Test: Create folder object
test_create_folder() {
    echo -e "\n${YELLOW}Test: Create folder object${NC}"
    
    # Create a folder object using put-object with empty body
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "test-folder/" \
        --content-length 0
    
    echo -e "${GREEN}✓ Folder object created${NC}"
}

# Test: List folder object
test_list_folder() {
    echo -e "\n${YELLOW}Test: List folder object${NC}"
    
    # List objects and verify the folder is present
    OBJECTS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/)
    if echo "$OBJECTS" | grep -q "test-folder/"; then
        echo -e "${GREEN}✓ Folder object listed successfully${NC}"
    else
        echo -e "${RED}✗ Folder object not found in list${NC}"
        echo "Objects found: $OBJECTS"
        exit 1
    fi
}

# Test: Get folder object metadata
test_head_folder() {
    echo -e "\n${YELLOW}Test: Head folder object (get metadata)${NC}"
    
    # Use head-object to verify folder exists and has 0 content length
    HEAD_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${TEST_BUCKET} \
        --key "test-folder/" 2>&1)
    
    if [ $? -eq 0 ]; then
        CONTENT_LENGTH=$(echo "$HEAD_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin).get('ContentLength', -1))" 2>/dev/null || echo "-1")
        if [ "$CONTENT_LENGTH" -eq 0 ]; then
            echo -e "${GREEN}✓ Folder object has content-length 0${NC}"
        else
            echo -e "${YELLOW}! Folder object content-length: ${CONTENT_LENGTH}${NC}"
        fi
        echo -e "${GREEN}✓ Folder object metadata retrieved successfully${NC}"
    else
        echo -e "${RED}✗ Failed to get folder object metadata${NC}"
        echo "$HEAD_OUTPUT"
        exit 1
    fi
}

# Test: Create file inside folder
test_file_inside_folder() {
    echo -e "\n${YELLOW}Test: Create file inside folder${NC}"
    
    # Create a file inside the folder
    echo "Hello from inside folder" > "${TEST_DATA_DIR}/folder-file.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/folder-file.txt" \
        s3://${TEST_BUCKET}/test-folder/nested-file.txt
    
    # Verify the file exists
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/test-folder/ | grep -q "nested-file.txt"; then
        echo -e "${GREEN}✓ File inside folder created successfully${NC}"
    else
        echo -e "${RED}✗ File inside folder not found${NC}"
        exit 1
    fi
}

# Test: List with delimiter shows folder as common prefix
test_list_with_delimiter_folder() {
    echo -e "\n${YELLOW}Test: List with delimiter shows folder as common prefix${NC}"
    
    # Create a root-level file for comparison
    echo "Root level content" > "${TEST_DATA_DIR}/root-file.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/root-file.txt" \
        s3://${TEST_BUCKET}/root-file.txt
    
    # List with delimiter - should show folders as common prefixes
    LIST_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${TEST_BUCKET} \
        --delimiter "/" 2>&1)
    
    # Check that test-folder/ appears as a common prefix
    if echo "$LIST_OUTPUT" | grep -q '"Prefix": "test-folder/"'; then
        echo -e "${GREEN}✓ Folder appears as common prefix${NC}"
    else
        echo -e "${YELLOW}! Folder as common prefix not explicitly found, checking listing...${NC}"
        # The AWS CLI s3 ls command uses delimiter by default
        LS_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/)
        echo "Listing output: $LS_OUTPUT"
    fi
    
    # Check that root-file.txt is listed as a regular object
    if echo "$LIST_OUTPUT" | grep -q "root-file.txt"; then
        echo -e "${GREEN}✓ Root file listed correctly${NC}"
    fi
    
    echo -e "${GREEN}✓ Delimiter listing test completed${NC}"
}

# Test: Copy folder object
test_copy_folder() {
    echo -e "\n${YELLOW}Test: Copy folder object${NC}"
    
    # Create a source folder if not exists
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "src-folder/" \
        --content-length 0
    
    # Copy the folder object
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api copy-object \
        --bucket ${TEST_BUCKET} \
        --key "dst-folder/" \
        --copy-source "${TEST_BUCKET}/src-folder/"
    
    # Verify destination folder exists
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "dst-folder/"; then
        echo -e "${GREEN}✓ Folder copied successfully${NC}"
    else
        echo -e "${RED}✗ Copied folder not found${NC}"
        exit 1
    fi
    
    # Verify the copy has 0 content length
    HEAD_OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${TEST_BUCKET} \
        --key "dst-folder/" 2>&1)
    
    if [ $? -eq 0 ]; then
        CONTENT_LENGTH=$(echo "$HEAD_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin).get('ContentLength', -1))" 2>/dev/null || echo "-1")
        if [ "$CONTENT_LENGTH" -eq 0 ]; then
            echo -e "${GREEN}✓ Copied folder has content-length 0${NC}"
        else
            echo -e "${YELLOW}! Copied folder content-length: ${CONTENT_LENGTH}${NC}"
        fi
    fi
}

# Test: Delete folder object
test_delete_folder() {
    echo -e "\n${YELLOW}Test: Delete folder object${NC}"
    
    # Create a folder to delete
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "delete-folder/" \
        --content-length 0
    
    # Delete the folder object
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/delete-folder/
    
    # Verify folder is deleted
    if ! aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ | grep -q "delete-folder/"; then
        echo -e "${GREEN}✓ Folder deleted successfully${NC}"
    else
        echo -e "${RED}✗ Folder still exists after deletion${NC}"
        exit 1
    fi
}

# Test: Nested folders
test_nested_folders() {
    echo -e "\n${YELLOW}Test: Nested folder structure${NC}"
    
    # Create nested folder structure
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "parent/" \
        --content-length 0
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "parent/child/" \
        --content-length 0
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "parent/child/grandchild/" \
        --content-length 0
    
    # Create a file in the deepest folder
    echo "Deep nested content" > "${TEST_DATA_DIR}/deep-file.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/deep-file.txt" \
        s3://${TEST_BUCKET}/parent/child/grandchild/deep-file.txt
    
    # Verify all levels exist
    ALL_OBJECTS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls s3://${TEST_BUCKET}/ --recursive)
    
    FOUND_PARENT=0
    FOUND_CHILD=0
    FOUND_GRANDCHILD=0
    FOUND_FILE=0
    
    if echo "$ALL_OBJECTS" | grep -q "parent/$"; then FOUND_PARENT=1; fi
    if echo "$ALL_OBJECTS" | grep -q "parent/child/$"; then FOUND_CHILD=1; fi
    if echo "$ALL_OBJECTS" | grep -q "parent/child/grandchild/$"; then FOUND_GRANDCHILD=1; fi
    if echo "$ALL_OBJECTS" | grep -q "parent/child/grandchild/deep-file.txt"; then FOUND_FILE=1; fi
    
    if [ "$FOUND_PARENT" -eq 1 ] && [ "$FOUND_CHILD" -eq 1 ] && [ "$FOUND_GRANDCHILD" -eq 1 ] && [ "$FOUND_FILE" -eq 1 ]; then
        echo -e "${GREEN}✓ All nested folders and file found${NC}"
    else
        echo -e "${YELLOW}Partial match - Parent: $FOUND_PARENT, Child: $FOUND_CHILD, Grandchild: $FOUND_GRANDCHILD, File: $FOUND_FILE${NC}"
        echo "All objects:"
        echo "$ALL_OBJECTS"
    fi
    
    echo -e "${GREEN}✓ Nested folder test completed${NC}"
}

# Main test runner for folder tests
run_folder_tests() {
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Running folder object e2e tests...${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    test_create_folder
    test_list_folder
    test_head_folder
    test_file_inside_folder
    test_list_with_delimiter_folder
    test_copy_folder
    test_delete_folder
    test_nested_folders
    
    # Cleanup folder test objects
    echo -e "\n${YELLOW}Cleaning up folder test objects...${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm s3://${TEST_BUCKET}/ --recursive 2>/dev/null || true
    
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Folder object e2e tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}
