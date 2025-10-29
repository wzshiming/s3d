#!/bin/bash
# E2E tests for AWS Signature V4 authentication

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load libraries
source "${SCRIPT_DIR}/lib/config.sh"
source "${SCRIPT_DIR}/lib/utils.sh"
source "${SCRIPT_DIR}/lib/server.sh"

# Initialize auth test data directories
export E2E_AUTH_TEST_DATA_DIR=$(create_temp_dir)
export E2E_AUTH_SERVER_DATA_DIR=$(create_temp_dir)

# Setup cleanup trap
trap cleanup_auth_server EXIT

# Test 1: Request without credentials should fail
test_auth_no_credentials() {
    test_header "Request without credentials (should fail)"
    
    # Temporarily unset credentials
    local OLD_ACCESS_KEY=$AWS_ACCESS_KEY_ID
    local OLD_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
    unset AWS_ACCESS_KEY_ID
    unset AWS_SECRET_ACCESS_KEY
    
    # This should fail with authentication error
    if curl -s -o /dev/null -w "%{http_code}" "${E2E_AUTH_SERVER_ADDR}/" | grep -q "403"; then
        assert_success "Request correctly rejected without credentials"
    else
        # Restore credentials before exiting
        export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
        export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
        assert_failure "Request should have been rejected"
    fi
    
    # Restore credentials
    export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
}

# Test 2: Request with wrong credentials should fail
test_auth_wrong_credentials() {
    test_header "Request with wrong credentials (should fail)"
    
    # Temporarily use wrong credentials
    local OLD_ACCESS_KEY=$AWS_ACCESS_KEY_ID
    local OLD_SECRET_KEY=$AWS_SECRET_ACCESS_KEY
    export AWS_ACCESS_KEY_ID="wrong-access-key"
    export AWS_SECRET_ACCESS_KEY="wrong-secret-key"
    
    # This should fail with authentication error
    if aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 ls 2>&1 | grep -q "403\|Forbidden\|SignatureDoesNotMatch"; then
        assert_success "Request correctly rejected with wrong credentials"
    else
        # Restore credentials before exiting
        export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
        export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
        assert_failure "Request should have been rejected"
    fi
    
    # Restore credentials
    export AWS_ACCESS_KEY_ID=$OLD_ACCESS_KEY
    export AWS_SECRET_ACCESS_KEY=$OLD_SECRET_KEY
}

# Test 3: Request with valid credentials should succeed
test_auth_valid_credentials() {
    test_header "Request with valid credentials (should succeed)"
    
    if aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 ls > /dev/null 2>&1; then
        assert_success "Request succeeded with valid credentials"
    else
        assert_failure "Request should have succeeded"
    fi
}

# Test 4: Create bucket with authentication
test_auth_create_bucket() {
    test_header "Create bucket with authentication"
    aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 mb "s3://${E2E_AUTH_TEST_BUCKET}"
    assert_success "Bucket created with authentication"
}

# Test 5: Upload object with authentication
test_auth_upload_object() {
    test_header "Upload object with authentication"
    echo "Test content with auth" > "${E2E_AUTH_TEST_DATA_DIR}/test-file.txt"
    aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 cp \
        "${E2E_AUTH_TEST_DATA_DIR}/test-file.txt" "s3://${E2E_AUTH_TEST_BUCKET}/test-file.txt"
    assert_success "Object uploaded with authentication"
}

# Test 6: Download object with authentication
test_auth_download_object() {
    test_header "Download object with authentication"
    aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 cp \
        "s3://${E2E_AUTH_TEST_BUCKET}/test-file.txt" "${E2E_AUTH_TEST_DATA_DIR}/downloaded.txt"
    
    if files_match "${E2E_AUTH_TEST_DATA_DIR}/test-file.txt" "${E2E_AUTH_TEST_DATA_DIR}/downloaded.txt"; then
        assert_success "Object downloaded correctly with authentication"
    else
        assert_failure "Downloaded content doesn't match"
    fi
}

# Test 7: List objects with authentication
test_auth_list_objects() {
    test_header "List objects with authentication"
    if object_exists "${E2E_AUTH_SERVER_ADDR}" "${E2E_AUTH_TEST_BUCKET}" "test-file.txt" "yes"; then
        assert_success "Objects listed successfully with authentication"
    else
        assert_failure "Object not found in list"
    fi
}

# Test 8: Delete object with authentication
test_auth_delete_object() {
    test_header "Delete object with authentication"
    aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 rm "s3://${E2E_AUTH_TEST_BUCKET}/test-file.txt"
    assert_success "Object deleted with authentication"
}

# Test 9: Delete bucket with authentication
test_auth_delete_bucket() {
    test_header "Delete bucket with authentication"
    aws --endpoint-url="${E2E_AUTH_SERVER_ADDR}" s3 rb "s3://${E2E_AUTH_TEST_BUCKET}"
    
    if ! bucket_exists "${E2E_AUTH_SERVER_ADDR}" "${E2E_AUTH_TEST_BUCKET}" "yes"; then
        assert_success "Bucket deleted with authentication"
    else
        assert_failure "Bucket still exists"
    fi
}

# Main execution
main() {
    section_header "AWS Signature V4 Authentication Tests"
    
    # Verify AWS CLI
    verify_aws_cli
    
    # Start auth server
    start_auth_server || exit 1
    
    # Run tests
    test_auth_no_credentials
    test_auth_wrong_credentials
    test_auth_valid_credentials
    test_auth_create_bucket
    test_auth_upload_object
    test_auth_download_object
    test_auth_list_objects
    test_auth_delete_object
    test_auth_delete_bucket
    
    section_success "All authentication e2e tests passed!"
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main
fi
