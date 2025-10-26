#!/bin/bash
set -e

# Main test runner
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/common.sh"
source "${SCRIPT_DIR}/bucket_tests.sh"
source "${SCRIPT_DIR}/object_tests.sh"
source "${SCRIPT_DIR}/advanced_tests.sh"

# Run setup
setup

# Run bucket tests
test_list_empty_buckets
test_create_bucket
test_list_buckets

# Run object tests
test_upload_file
test_list_objects
test_download_file
test_upload_multiple_files
test_list_with_prefix
test_copy_object
test_delete_object

# Run advanced tests
test_multipart_upload
test_sync_directory

# Cleanup objects
test_remove_all_objects

# Cleanup bucket
test_delete_bucket

# Run nested path test
test_nested_paths

# Clean up final test
aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${TEST_BUCKET} --force

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}All e2e tests passed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"

exit 0
