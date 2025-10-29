#!/bin/bash
# Tests for bucket operations

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/config.sh"
source "${SCRIPT_DIR}/lib/utils.sh"

# Test 1: List buckets (should be empty)
test_list_empty_buckets() {
    test_header "List buckets (should be empty)"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 ls
    assert_success "List buckets successful"
}

# Test 2: Create bucket
test_create_bucket() {
    test_header "Create bucket"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 mb "s3://${E2E_TEST_BUCKET}"
    assert_success "Bucket created"
}

# Test 3: List buckets (should show our bucket)
test_list_buckets() {
    test_header "List buckets (should show our bucket)"
    if bucket_exists "${E2E_SERVER_ADDR}" "${E2E_TEST_BUCKET}"; then
        assert_success "Bucket listed successfully"
    else
        assert_failure "Bucket not found in list"
    fi
}

# Test 4: Delete bucket
test_delete_bucket() {
    test_header "Delete bucket"
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 rb "s3://${E2E_TEST_BUCKET}"
    
    if ! bucket_exists "${E2E_SERVER_ADDR}" "${E2E_TEST_BUCKET}"; then
        assert_success "Bucket deleted"
    else
        assert_failure "Bucket still exists"
    fi
}
