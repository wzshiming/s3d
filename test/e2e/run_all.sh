#!/bin/bash
set -e

# Main test runner
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/common.sh"
source "${SCRIPT_DIR}/bucket_tests.sh"
source "${SCRIPT_DIR}/bucket_pagination_tests.sh"
source "${SCRIPT_DIR}/object_tests.sh"
source "${SCRIPT_DIR}/advanced_tests.sh"
source "${SCRIPT_DIR}/duplicate_write_tests.sh"
source "${SCRIPT_DIR}/nonascii_tests.sh"

# Run setup
setup

# Run bucket tests
test_list_empty_buckets
test_create_bucket
test_list_buckets

# Run bucket pagination tests
run_bucket_pagination_tests

# Run object tests
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
test_put_duplicate_same_content
test_put_duplicate_different_content
test_copy_to_existing_same_content
test_copy_to_existing_different_content
test_rename_to_existing_same_content
test_rename_to_existing_different_content

# Run non-ASCII object key tests
test_nonascii_object_keys

# Run advanced tests
test_multipart_upload
test_sync_directory
test_upload_part_copy

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

# Run authentication tests
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}Running authentication e2e tests...${NC}"
echo -e "${YELLOW}========================================${NC}"
"${SCRIPT_DIR}/auth_tests.sh"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}All e2e tests (including auth) passed!${NC}"
echo -e "${GREEN}========================================${NC}"

exit 0
