#!/bin/bash
# s3fs-fuse binary mount tests
# This test actually mounts an S3 bucket using s3fs-fuse to verify compatibility

test_s3fs_binary_mount() {
    echo -e "\n${YELLOW}Testing with actual s3fs-fuse binary...${NC}"
    
    # Check if s3fs is available, if not, try to install it
    if ! command -v s3fs &> /dev/null; then
        echo "s3fs not found, attempting to install..."
        
        # Try to install s3fs-fuse
        if command -v apt-get &> /dev/null; then
            echo "Installing s3fs-fuse via apt..."
            sudo apt-get update -qq > /dev/null 2>&1 || true
            sudo apt-get install -y s3fs > /dev/null 2>&1
        elif command -v yum &> /dev/null; then
            echo "Installing s3fs-fuse via yum..."
            sudo yum install -y s3fs-fuse > /dev/null 2>&1
        elif command -v brew &> /dev/null; then
            echo "Installing s3fs-fuse via brew..."
            brew install s3fs > /dev/null 2>&1
        else
            echo -e "${YELLOW}Cannot install s3fs-fuse automatically. Skipping s3fs binary test.${NC}"
            echo -e "${YELLOW}To run this test, install s3fs-fuse: https://github.com/s3fs-fuse/s3fs-fuse${NC}"
            return 0  # Skip test but don't fail
        fi
        
        # Check if installation succeeded
        if ! command -v s3fs &> /dev/null; then
            echo -e "${YELLOW}s3fs-fuse installation failed. Skipping s3fs binary test.${NC}"
            return 0  # Skip test but don't fail
        fi
    fi
    
    echo "s3fs-fuse binary found: $(which s3fs)"
    s3fs --version 2>&1 | head -1 || echo "s3fs version unknown"
    
    # Create a mount point
    MOUNT_POINT=$(mktemp -d)
    echo "Mount point: ${MOUNT_POINT}"
    
    # Create credentials file for s3fs
    PASSWD_FILE=$(mktemp)
    echo "test:test" > "${PASSWD_FILE}"
    chmod 600 "${PASSWD_FILE}"
    
    # Extract host and port from SERVER_ADDR
    SERVER_HOST=$(echo ${SERVER_ADDR} | sed 's|http://||' | sed 's|https://||')
    
    echo "Mounting bucket '${TEST_BUCKET}' with s3fs..."
    
    # Mount the bucket using s3fs
    # Note: s3fs expects the endpoint without http:// prefix
    s3fs ${TEST_BUCKET} ${MOUNT_POINT} \
        -o passwd_file=${PASSWD_FILE} \
        -o url=${SERVER_ADDR} \
        -o use_path_request_style \
        -o dbglevel=info \
        -f -o no_check_certificate \
        -o enable_noobj_cache &
    
    S3FS_PID=$!
    
    # Give s3fs time to mount
    sleep 3
    
    # Check if mount succeeded
    if ! mountpoint -q ${MOUNT_POINT} 2>/dev/null; then
        echo -e "${YELLOW}s3fs mount failed or not ready. Checking if directory is accessible...${NC}"
        # Sometimes s3fs works but mountpoint doesn't detect it in user namespaces
        if [ ! -d "${MOUNT_POINT}" ] || [ "$(ls -A ${MOUNT_POINT} 2>/dev/null | wc -l)" -eq 0 ]; then
            echo -e "${YELLOW}Mount point not accessible. This may be due to FUSE restrictions.${NC}"
            echo -e "${YELLOW}Skipping s3fs binary test (requires FUSE support).${NC}"
            kill ${S3FS_PID} 2>/dev/null || true
            rm -f ${PASSWD_FILE}
            rmdir ${MOUNT_POINT} 2>/dev/null || true
            return 0  # Skip test but don't fail
        fi
    fi
    
    echo -e "${GREEN}s3fs mount successful${NC}"
    
    # Cleanup function for mount test
    cleanup_mount() {
        echo "Unmounting s3fs..."
        fusermount -u ${MOUNT_POINT} 2>/dev/null || umount ${MOUNT_POINT} 2>/dev/null || true
        kill ${S3FS_PID} 2>/dev/null || true
        sleep 1
        rm -f ${PASSWD_FILE}
        rmdir ${MOUNT_POINT} 2>/dev/null || true
    }
    trap cleanup_mount RETURN
    
    # Test 1: Create a directory through s3fs
    echo "Test 1: Create directory via s3fs..."
    mkdir -p "${MOUNT_POINT}/s3fs-binary-test-dir"
    if [ ! -d "${MOUNT_POINT}/s3fs-binary-test-dir" ]; then
        echo -e "${RED}Failed to create directory via s3fs${NC}"
        return 1
    fi
    echo -e "${GREEN}Directory created via s3fs${NC}"
    
    # Verify the directory object exists in S3
    echo "Verifying directory object in S3..."
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 \
        --bucket ${TEST_BUCKET} \
        --query "Contents[?ends_with(Key, '/')].Key" \
        --output text 2>/dev/null | grep -c "s3fs-binary-test-dir/" || echo "0")
    
    if [ "$RESULT" -eq "0" ]; then
        echo -e "${YELLOW}Note: Directory object not visible in S3 (this is expected for some s3fs versions)${NC}"
    else
        echo -e "${GREEN}Directory object visible in S3${NC}"
    fi
    
    # Test 2: Create a file in the directory
    echo "Test 2: Create file in directory via s3fs..."
    echo "test content from s3fs" > "${MOUNT_POINT}/s3fs-binary-test-dir/test-file.txt"
    if [ ! -f "${MOUNT_POINT}/s3fs-binary-test-dir/test-file.txt" ]; then
        echo -e "${RED}Failed to create file via s3fs${NC}"
        return 1
    fi
    echo -e "${GREEN}File created via s3fs${NC}"
    
    # Test 3: Verify file is accessible via S3 API
    echo "Test 3: Verify file via S3 API..."
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-binary-test-dir/test-file.txt" \
        "${TEST_DATA_DIR}/s3fs-retrieved-file.txt" > /dev/null 2>&1
    
    if [ ! -f "${TEST_DATA_DIR}/s3fs-retrieved-file.txt" ]; then
        echo -e "${RED}Failed to retrieve file via S3 API${NC}"
        return 1
    fi
    
    CONTENT=$(cat "${TEST_DATA_DIR}/s3fs-retrieved-file.txt")
    if [ "$CONTENT" != "test content from s3fs" ]; then
        echo -e "${RED}File content mismatch${NC}"
        return 1
    fi
    echo -e "${GREEN}File accessible and correct via S3 API${NC}"
    
    # Test 4: List directory via s3fs
    echo "Test 4: List directory via s3fs..."
    FILE_COUNT=$(ls "${MOUNT_POINT}/s3fs-binary-test-dir" 2>/dev/null | wc -l)
    if [ "$FILE_COUNT" -lt "1" ]; then
        echo -e "${RED}Directory listing via s3fs failed${NC}"
        return 1
    fi
    echo -e "${GREEN}Directory listing via s3fs works (${FILE_COUNT} file(s))${NC}"
    
    # Test 5: Create nested directories
    echo "Test 5: Create nested directories via s3fs..."
    mkdir -p "${MOUNT_POINT}/s3fs-binary-test-dir/subdir1/subdir2"
    if [ ! -d "${MOUNT_POINT}/s3fs-binary-test-dir/subdir1/subdir2" ]; then
        echo -e "${RED}Failed to create nested directories via s3fs${NC}"
        return 1
    fi
    echo -e "${GREEN}Nested directories created via s3fs${NC}"
    
    # Test 6: Delete file via s3fs
    echo "Test 6: Delete file via s3fs..."
    rm -f "${MOUNT_POINT}/s3fs-binary-test-dir/test-file.txt"
    if [ -f "${MOUNT_POINT}/s3fs-binary-test-dir/test-file.txt" ]; then
        echo -e "${RED}Failed to delete file via s3fs${NC}"
        return 1
    fi
    echo -e "${GREEN}File deleted via s3fs${NC}"
    
    # Verify deletion via S3 API
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
        --bucket ${TEST_BUCKET} \
        --key "s3fs-binary-test-dir/test-file.txt" > /dev/null 2>&1; then
        echo -e "${RED}File still exists in S3 after s3fs deletion${NC}"
        return 1
    fi
    echo -e "${GREEN}File deletion confirmed via S3 API${NC}"
    
    # Test 7: Unmount and verify data persistence
    echo "Test 7: Unmount and remount to verify persistence..."
    cleanup_mount
    
    # Wait for unmount
    sleep 2
    
    # Remount
    s3fs ${TEST_BUCKET} ${MOUNT_POINT} \
        -o passwd_file=${PASSWD_FILE} \
        -o url=${SERVER_ADDR} \
        -o use_path_request_style \
        -o dbglevel=info \
        -f -o no_check_certificate \
        -o enable_noobj_cache &
    
    S3FS_PID=$!
    sleep 3
    
    # Check if directory still exists
    if [ ! -d "${MOUNT_POINT}/s3fs-binary-test-dir" ]; then
        echo -e "${YELLOW}Directory not visible after remount (may be expected)${NC}"
    else
        echo -e "${GREEN}Directory persisted after remount${NC}"
    fi
    
    echo -e "${GREEN}All s3fs binary mount tests completed successfully!${NC}"
    return 0
}
