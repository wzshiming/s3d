#!/bin/bash
# Tests for basic object operations

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/config.sh"
source "${SCRIPT_DIR}/../lib/utils.sh"

# Test: Upload a file
test_upload_file() {
    test_header "Upload a file"
    echo "Hello, S3!" > "${E2E_TEST_DATA_DIR}/test-file.txt"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 cp \
        "${E2E_TEST_DATA_DIR}/test-file.txt" "s3://${E2E_TEST_BUCKET}/test-file.txt"
    assert_success "File uploaded"
}

# Test: List objects
test_list_objects() {
    test_header "List objects"
    if object_exists "${E2E_SERVER_ADDR}" "${E2E_TEST_BUCKET}" "test-file.txt"; then
        assert_success "Object listed successfully"
    else
        assert_failure "Object not found in list"
    fi
}

# Test: Download the file
test_download_file() {
    test_header "Download the file"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 cp \
        "s3://${E2E_TEST_BUCKET}/test-file.txt" "${E2E_TEST_DATA_DIR}/downloaded.txt"
    
    if files_match "${E2E_TEST_DATA_DIR}/test-file.txt" "${E2E_TEST_DATA_DIR}/downloaded.txt"; then
        assert_success "File downloaded and verified"
    else
        assert_failure "Downloaded file does not match original"
    fi
}

# Test: Upload multiple files
test_upload_multiple_files() {
    test_header "Upload multiple files"
    for i in {1..5}; do
        echo "Test content $i" > "${E2E_TEST_DATA_DIR}/file-${i}.txt"
    done
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 cp \
        "${E2E_TEST_DATA_DIR}/" "s3://${E2E_TEST_BUCKET}/files/" --recursive
    assert_success "Multiple files uploaded"
}

# Test: List objects with prefix
test_list_with_prefix() {
    test_header "List objects with prefix"
    local objects=$(aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 ls "s3://${E2E_TEST_BUCKET}/files/")
    local file_count=$(echo "$objects" | grep -c "file-" || true)
    
    if [ "$file_count" -eq 5 ]; then
        assert_success "All files listed with prefix"
    else
        assert_failure "Expected 5 files, found ${file_count}"
    fi
}

# Test: Copy object
test_copy_object() {
    test_header "Copy object"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 cp \
        "s3://${E2E_TEST_BUCKET}/test-file.txt" "s3://${E2E_TEST_BUCKET}/test-file-copy.txt"
    
    if object_exists "${E2E_SERVER_ADDR}" "${E2E_TEST_BUCKET}" "test-file-copy.txt"; then
        assert_success "Object copied successfully"
    else
        assert_failure "Copied object not found"
    fi
}

# Test: Delete object
test_delete_object() {
    test_header "Delete object"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 rm \
        "s3://${E2E_TEST_BUCKET}/test-file-copy.txt"
    
    if ! object_exists "${E2E_SERVER_ADDR}" "${E2E_TEST_BUCKET}" "test-file-copy.txt"; then
        assert_success "Object deleted successfully"
    else
        assert_failure "Object still exists after deletion"
    fi
}

# Test: Remove all objects
test_remove_all_objects() {
    test_header "Remove all objects"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 rm \
        "s3://${E2E_TEST_BUCKET}/" --recursive
    
    local remaining=$(aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 ls \
        "s3://${E2E_TEST_BUCKET}/" | wc -l)
    
    if [ "$remaining" -eq 0 ]; then
        assert_success "All objects removed"
    else
        assert_failure "${remaining} objects still remain"
    fi
}
