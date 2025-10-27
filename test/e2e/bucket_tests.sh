#!/bin/bash
# Tests for bucket operations

source "$(dirname "$0")/common.sh"

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

# Test 4: Delete bucket
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

# Test 5: ListBuckets pagination
test_list_buckets_pagination() {
    echo -e "\n${YELLOW}Test: ListBuckets pagination${NC}"
    
    # Create multiple buckets
    for i in {01..10}; do
        BUCKET_NAME="test-pagination-bucket-${i}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://${BUCKET_NAME}" > /dev/null
    done
    
    # Test pagination with max-buckets
    echo -e "${YELLOW}  Testing pagination with max-buckets=3${NC}"
    RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets --max-buckets 3)
    
    # Check if IsTruncated is true
    IS_TRUNCATED=$(echo "$RESULT" | grep -o '"IsTruncated": *true' || true)
    if [ -n "$IS_TRUNCATED" ]; then
        echo -e "${GREEN}  ✓ IsTruncated=true for paginated results${NC}"
    else
        echo -e "${RED}  ✗ IsTruncated should be true${NC}"
    fi
    
    # Check if ContinuationToken exists
    CONT_TOKEN=$(echo "$RESULT" | grep -o '"ContinuationToken": *"[^"]*"' | cut -d'"' -f4)
    if [ -n "$CONT_TOKEN" ]; then
        echo -e "${GREEN}  ✓ ContinuationToken found: ${CONT_TOKEN}${NC}"
    else
        echo -e "${YELLOW}  ⚠ No ContinuationToken (may be on last page)${NC}"
    fi
    
    # If we have a continuation token, fetch the next page
    if [ -n "$CONT_TOKEN" ]; then
        echo -e "${YELLOW}  Fetching next page with continuation token${NC}"
        RESULT2=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets --max-buckets 3 --continuation-token "${CONT_TOKEN}")
        
        BUCKET_COUNT=$(echo "$RESULT2" | grep -o '"Name"' | wc -l)
        echo -e "${GREEN}  ✓ Second page returned ${BUCKET_COUNT} buckets${NC}"
    fi
    
    # Test iteration through all buckets
    echo -e "${YELLOW}  Testing iteration through all buckets${NC}"
    TOTAL_BUCKETS=0
    CONT_TOKEN=""
    MAX_ITERATIONS=20
    ITERATION=0
    
    while [ $ITERATION -lt $MAX_ITERATIONS ]; do
        if [ -z "$CONT_TOKEN" ]; then
            RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets --max-buckets 2)
        else
            RESULT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-buckets --max-buckets 2 --continuation-token "${CONT_TOKEN}")
        fi
        
        PAGE_COUNT=$(echo "$RESULT" | grep -o '"Name"' | wc -l)
        TOTAL_BUCKETS=$((TOTAL_BUCKETS + PAGE_COUNT))
        
        CONT_TOKEN=$(echo "$RESULT" | grep -o '"ContinuationToken": *"[^"]*"' | cut -d'"' -f4)
        
        ITERATION=$((ITERATION + 1))
        
        if [ -z "$CONT_TOKEN" ]; then
            break
        fi
    done
    
    if [ $TOTAL_BUCKETS -ge 10 ]; then
        echo -e "${GREEN}  ✓ Successfully iterated through ${TOTAL_BUCKETS} buckets${NC}"
    else
        echo -e "${RED}  ✗ Expected at least 10 buckets, got ${TOTAL_BUCKETS}${NC}"
        exit 1
    fi
    
    # Cleanup test buckets
    for i in {01..10}; do
        BUCKET_NAME="test-pagination-bucket-${i}"
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb "s3://${BUCKET_NAME}" 2>/dev/null || true
    done
    
    echo -e "${GREEN}✓ ListBuckets pagination test completed${NC}"
}
