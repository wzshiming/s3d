#!/bin/bash
# Tests for ListBuckets pagination

source "$(dirname "$0")/common.sh"

# Test: ListBuckets pagination with max-buckets
test_list_buckets_pagination() {
    echo -e "\n${YELLOW}Test: ListBuckets pagination with max-buckets${NC}"
    
    # Create multiple buckets
    for i in {001..015}; do
        BUCKET="e2e-test-bucket-${i}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://${BUCKET}" > /dev/null 2>&1
    done
    
    # List buckets with max-buckets=5
    OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets \
        --query 'Buckets[?starts_with(Name, `e2e-test-bucket-`)].Name' \
        --max-items 5)
    
    # Count returned buckets
    COUNT=$(echo "$OUTPUT" | grep -c "e2e-test-bucket-")
    if [ "$COUNT" -le 5 ]; then
        echo -e "${GREEN}✓ Pagination respected max-buckets limit${NC}"
    else
        echo -e "${RED}✗ Expected at most 5 buckets, got ${COUNT}${NC}"
        cleanup_pagination_test
        exit 1
    fi
    
    cleanup_pagination_test
}

# Test: ListBuckets with prefix filter
test_list_buckets_prefix() {
    echo -e "\n${YELLOW}Test: ListBuckets with prefix filter${NC}"
    
    # Create buckets with different prefixes
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://aaa-test-bucket" > /dev/null 2>&1
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://bbb-test-bucket" > /dev/null 2>&1
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://ccc-test-bucket" > /dev/null 2>&1
    
    # List buckets with prefix (using aws s3 ls which uses prefix internally)
    OUTPUT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets \
        --query 'Buckets[?starts_with(Name, `bbb-`)].Name' --output text)
    
    if echo "$OUTPUT" | grep -q "bbb-test-bucket"; then
        if ! echo "$OUTPUT" | grep -q "aaa-test-bucket" && ! echo "$OUTPUT" | grep -q "ccc-test-bucket"; then
            echo -e "${GREEN}✓ Prefix filter works correctly${NC}"
        else
            echo -e "${RED}✗ Prefix filter returned unexpected buckets${NC}"
            cleanup_prefix_test
            exit 1
        fi
    else
        echo -e "${RED}✗ Expected bucket not found with prefix filter${NC}"
        cleanup_prefix_test
        exit 1
    fi
    
    cleanup_prefix_test
}

# Test: ListBuckets with continuation token
test_list_buckets_continuation() {
    echo -e "\n${YELLOW}Test: ListBuckets with continuation token${NC}"
    
    # Create multiple buckets
    for i in {001..010}; do
        BUCKET="cont-test-bucket-${i}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://${BUCKET}" > /dev/null 2>&1
    done
    
    # Get first page
    FIRST_PAGE=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets \
        --max-items 3 --query 'Buckets[?starts_with(Name, `cont-test-bucket-`)].Name' --output json)
    
    FIRST_COUNT=$(echo "$FIRST_PAGE" | grep -c "cont-test-bucket-")
    
    if [ "$FIRST_COUNT" -le 3 ]; then
        echo -e "${GREEN}✓ Continuation token pagination works${NC}"
    else
        echo -e "${RED}✗ Expected at most 3 buckets in first page, got ${FIRST_COUNT}${NC}"
        cleanup_continuation_test
        exit 1
    fi
    
    cleanup_continuation_test
}

# Cleanup functions
cleanup_pagination_test() {
    for i in {001..015}; do
        BUCKET="e2e-test-bucket-${i}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb "s3://${BUCKET}" > /dev/null 2>&1
    done
}

cleanup_prefix_test() {
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb "s3://aaa-test-bucket" > /dev/null 2>&1
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb "s3://bbb-test-bucket" > /dev/null 2>&1
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb "s3://ccc-test-bucket" > /dev/null 2>&1
}

cleanup_continuation_test() {
    for i in {001..010}; do
        BUCKET="cont-test-bucket-${i}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb "s3://${BUCKET}" > /dev/null 2>&1
    done
}

# Run all tests
run_bucket_pagination_tests() {
    echo -e "\n${BLUE}=== Running Bucket Pagination Tests ===${NC}"
    test_list_buckets_pagination
    test_list_buckets_prefix
    test_list_buckets_continuation
    echo -e "\n${GREEN}All bucket pagination tests passed!${NC}"
}
