#!/bin/bash
# Modular test runner for e2e tests
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load libraries
source "${SCRIPT_DIR}/lib/config.sh"
source "${SCRIPT_DIR}/lib/utils.sh"
source "${SCRIPT_DIR}/lib/server.sh"

# Initialize test data directories
export E2E_TEST_DATA_DIR=$(create_temp_dir)
export E2E_SERVER_DATA_DIR=$(create_temp_dir)

# Setup cleanup trap
trap cleanup_server EXIT

# Function to run a test suite
run_test_suite() {
    local suite_name="$1"
    local suite_file="$2"
    
    section_header "Running ${suite_name}"
    source "${suite_file}"
}

# Main test execution
main() {
    section_header "S3D End-to-End Test Suite"
    
    # Verify prerequisites
    verify_aws_cli
    
    # Start server
    start_server || exit 1
    
    # Import test files
    source "${SCRIPT_DIR}/bucket_tests.sh"
    source "${SCRIPT_DIR}/object_tests.sh"
    source "${SCRIPT_DIR}/advanced_tests.sh"
    source "${SCRIPT_DIR}/duplicate_write_tests.sh"
    
    # Run bucket tests
    section_header "Bucket Operations"
    test_list_empty_buckets
    test_create_bucket
    test_list_buckets
    
    # Run basic object tests
    section_header "Basic Object Operations"
    test_upload_file
    test_list_objects
    test_download_file
    test_upload_multiple_files
    test_list_with_prefix
    test_copy_object
    test_rename_object
    test_delete_object
    test_delete_objects
    
    # Run duplicate write compatibility tests
    section_header "Duplicate Write Compatibility"
    test_put_duplicate_same_content
    test_put_duplicate_different_content
    test_copy_to_existing_same_content
    test_copy_to_existing_different_content
    test_rename_to_existing_same_content
    test_rename_to_existing_different_content
    
    # Run advanced tests
    section_header "Advanced Features"
    test_multipart_upload
    test_sync_directory
    test_upload_part_copy
    
    # Cleanup objects
    test_remove_all_objects
    
    # Cleanup bucket
    test_delete_bucket
    
    # Run nested path test
    test_nested_paths
    
    # Clean up final test bucket
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 rb "s3://${E2E_TEST_BUCKET}" --force 2>/dev/null || true
    
    section_success "All e2e tests passed successfully!"
}

# Run main if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main
    exit 0
fi
