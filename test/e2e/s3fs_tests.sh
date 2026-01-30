#!/bin/bash
# s3fs-fuse compatibility tests

# Test s3fs-fuse compatibility with directory objects

test_s3fs_directory_support() {
    echo -e "\n${YELLOW}Testing s3fs-fuse directory support...${NC}"
    
    # Create a directory object (zero-byte object with trailing slash)
    echo "Creating directory object..."
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-test-dir/" \
        --content-length 0
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to create directory object${NC}"
        return 1
    fi
    echo -e "${GREEN}Directory object created${NC}"
    
    # Verify the directory object shows in listing
    echo "Listing objects to verify directory..."
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${TEST_BUCKET} \
        --query "Contents[?Key=='s3fs-test-dir/'].Key" \
        --output text)
    
    if [ "$RESULT" != "s3fs-test-dir/" ]; then
        echo -e "${RED}Directory object not found in listing${NC}"
        return 1
    fi
    echo -e "${GREEN}Directory object found in listing${NC}"
    
    # Test HEAD request on directory object
    echo "Testing HEAD request on directory..."
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-test-dir/" > /dev/null
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}HEAD request on directory failed${NC}"
        return 1
    fi
    echo -e "${GREEN}HEAD request on directory successful${NC}"
    
    # Test GET request on directory object
    echo "Testing GET request on directory..."
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-test-dir/" \
        "${TEST_DATA_DIR}/dir-output" > /dev/null
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}GET request on directory failed${NC}"
        return 1
    fi
    
    # Verify the content is empty
    SIZE=$(wc -c < "${TEST_DATA_DIR}/dir-output")
    if [ "$SIZE" != "0" ]; then
        echo -e "${RED}Directory object should be 0 bytes, got ${SIZE}${NC}"
        return 1
    fi
    echo -e "${GREEN}GET request on directory successful (0 bytes)${NC}"
    
    # Create a file under the directory
    echo "Creating file under directory..."
    echo "test content" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - \
        s3://${TEST_BUCKET}/s3fs-test-dir/file.txt
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to create file under directory${NC}"
        return 1
    fi
    echo -e "${GREEN}File created under directory${NC}"
    
    # List with delimiter to show directory structure
    echo "Testing list with delimiter..."
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${TEST_BUCKET} \
        --delimiter "/" \
        --query "CommonPrefixes[?Prefix=='s3fs-test-dir/'].Prefix" \
        --output text)
    
    if [ "$RESULT" != "s3fs-test-dir/" ]; then
        echo -e "${RED}Directory not found in common prefixes${NC}"
        return 1
    fi
    echo -e "${GREEN}List with delimiter successful${NC}"
    
    # Test nested directories
    echo "Testing nested directories..."
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "level1/" \
        --content-length 0 > /dev/null
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "level1/level2/" \
        --content-length 0 > /dev/null
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "level1/level2/level3/" \
        --content-length 0 > /dev/null
    
    # List with prefix to see nested structure
    COUNT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${TEST_BUCKET} \
        --prefix "level1/" \
        --query "length(Contents[?ends_with(Key, '/')])" \
        --output text)
    
    if [ "$COUNT" != "3" ]; then
        echo -e "${RED}Expected 3 nested directories (level1/, level2/, level3/), got ${COUNT}${NC}"
        return 1
    fi
    echo -e "${GREEN}Nested directories test successful${NC}"
    
    # Test distinction between file and directory with same name
    echo "Testing file vs directory distinction..."
    echo "file content" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - \
        s3://${TEST_BUCKET}/samename
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api put-object \
        --bucket ${TEST_BUCKET} \
        --key "samename/" \
        --content-length 0 > /dev/null
    
    # Both should exist and be distinguishable
    COUNT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${TEST_BUCKET} \
        --query "length(Contents[?starts_with(Key, 'samename')])" \
        --output text)
    
    if [ "$COUNT" != "2" ]; then
        echo -e "${RED}Expected 2 objects (file and directory), got ${COUNT}${NC}"
        return 1
    fi
    echo -e "${GREEN}File vs directory distinction test successful${NC}"
    
    # Delete directory objects
    echo "Testing deletion of directory objects..."
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api delete-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-test-dir/" > /dev/null
    
    # Verify it's deleted
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-test-dir/" 2>&1)
    
    if ! echo "$RESULT" | grep -q "404\|Not Found"; then
        echo -e "${RED}Directory object should be deleted${NC}"
        return 1
    fi
    echo -e "${GREEN}Directory deletion successful${NC}"
    
    echo -e "${GREEN}All s3fs-fuse compatibility tests passed!${NC}"
    return 0
}
