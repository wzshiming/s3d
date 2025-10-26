#!/bin/bash
# Tests for bucket operations

set -e

# Source common utilities if not already sourced
if [ -z "$SERVER_ADDR" ]; then
    source "$(dirname "$0")/common.sh"
fi

# Test 1: List buckets (should be empty)
test_list_empty_buckets() {
    echo -e "\n${YELLOW}Test: List buckets (should be empty)${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls
    echo -e "${GREEN}✓ List buckets successful${NC}"
}

# Test 2: Create bucket
test_create_bucket() {
    echo -e "\n${YELLOW}Test: Create bucket${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET}
    echo -e "${GREEN}✓ Bucket created${NC}"
}

# Test 3: List buckets (should show our bucket)
test_list_buckets() {
    echo -e "\n${YELLOW}Test: List buckets (should show our bucket)${NC}"
    BUCKETS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls)
    if echo "$BUCKETS" | grep -q "${TEST_BUCKET}"; then
        echo -e "${GREEN}✓ Bucket listed successfully${NC}"
    else
        echo -e "${RED}✗ Bucket not found in list${NC}"
        exit 1
    fi
}

# Test 14: Delete bucket
test_delete_bucket() {
    echo -e "\n${YELLOW}Test: Delete bucket${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${TEST_BUCKET}
    BUCKETS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls)
    if ! echo "$BUCKETS" | grep -q "${TEST_BUCKET}"; then
        echo -e "${GREEN}✓ Bucket deleted${NC}"
    else
        echo -e "${RED}✗ Bucket still exists${NC}"
        exit 1
    fi
}

# Run tests if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    setup
    
    test_list_empty_buckets
    test_create_bucket
    test_list_buckets
    test_delete_bucket
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Bucket tests passed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
fi
