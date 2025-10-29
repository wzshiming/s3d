#!/bin/bash
# E2E tests for HeadBucket with region header and bucket ownership controls

source "$(dirname "$0")/common.sh"

# Test: HeadBucket returns region header
test_head_bucket_region() {
    echo -e "\n${YELLOW}Test: HeadBucket returns region header${NC}"
    
    # Create bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://test-region-bucket
    
    # Use curl to check for region header
    RESPONSE=$(curl -I "${SERVER_ADDR}/test-region-bucket" 2>&1)
    if echo "$RESPONSE" | grep -i "x-amz-bucket-region: us-east-1"; then
        echo -e "${GREEN}✓ HeadBucket returns region header${NC}"
    else
        echo -e "${RED}✗ Region header not found in HeadBucket response${NC}"
        echo "$RESPONSE"
        exit 1
    fi
    
    # Cleanup
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://test-region-bucket
}

# Test: Get bucket ownership controls (default)
test_get_ownership_default() {
    echo -e "\n${YELLOW}Test: Get bucket ownership controls (default)${NC}"
    
    # Create bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://test-ownership-bucket
    
    # Get ownership controls using curl
    RESPONSE=$(curl -s "${SERVER_ADDR}/test-ownership-bucket?ownershipControls")
    if echo "$RESPONSE" | grep -q "BucketOwnerEnforced"; then
        echo -e "${GREEN}✓ Default ownership is BucketOwnerEnforced${NC}"
    else
        echo -e "${RED}✗ Unexpected ownership response${NC}"
        echo "$RESPONSE"
        exit 1
    fi
}

# Test: Put bucket ownership controls
test_put_ownership() {
    echo -e "\n${YELLOW}Test: Put bucket ownership controls${NC}"
    
    # Create ownership controls XML
    cat > /tmp/ownership.xml << 'EOF'
<OwnershipControls>
  <Rule>
    <ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>
  </Rule>
</OwnershipControls>
EOF
    
    # Put ownership controls
    RESPONSE=$(curl -s -X PUT "${SERVER_ADDR}/test-ownership-bucket?ownershipControls" -d @/tmp/ownership.xml)
    
    # Get ownership controls to verify
    RESPONSE=$(curl -s "${SERVER_ADDR}/test-ownership-bucket?ownershipControls")
    if echo "$RESPONSE" | grep -q "BucketOwnerPreferred"; then
        echo -e "${GREEN}✓ Ownership updated to BucketOwnerPreferred${NC}"
    else
        echo -e "${RED}✗ Ownership not updated${NC}"
        echo "$RESPONSE"
        exit 1
    fi
    
    rm -f /tmp/ownership.xml
}

# Test: Delete bucket ownership controls
test_delete_ownership() {
    echo -e "\n${YELLOW}Test: Delete bucket ownership controls${NC}"
    
    # Delete ownership controls
    curl -s -X DELETE "${SERVER_ADDR}/test-ownership-bucket?ownershipControls" > /dev/null
    
    # Get ownership controls to verify (should revert to default)
    RESPONSE=$(curl -s "${SERVER_ADDR}/test-ownership-bucket?ownershipControls")
    if echo "$RESPONSE" | grep -q "BucketOwnerEnforced"; then
        echo -e "${GREEN}✓ Ownership reverted to default (BucketOwnerEnforced)${NC}"
    else
        echo -e "${RED}✗ Ownership not reverted to default${NC}"
        echo "$RESPONSE"
        exit 1
    fi
    
    # Cleanup
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://test-ownership-bucket
}

# Test: Region header in all responses
test_region_header_in_all_responses() {
    echo -e "\n${YELLOW}Test: Region header in all responses${NC}"
    
    # Create bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://test-region-all-bucket
    
    # Test CreateBucket response
    RESPONSE=$(curl -I -X PUT "${SERVER_ADDR}/test-region-create-bucket" 2>&1)
    if echo "$RESPONSE" | grep -i "x-amz-bucket-region: us-east-1"; then
        echo -e "${GREEN}✓ CreateBucket returns region header${NC}"
    else
        echo -e "${RED}✗ Region header not found in CreateBucket response${NC}"
        exit 1
    fi
    
    # Test ListObjects response
    RESPONSE=$(curl -I "${SERVER_ADDR}/test-region-all-bucket" 2>&1)
    if echo "$RESPONSE" | grep -i "x-amz-bucket-region: us-east-1"; then
        echo -e "${GREEN}✓ ListObjects returns region header${NC}"
    else
        echo -e "${RED}✗ Region header not found in ListObjects response${NC}"
        exit 1
    fi
    
    # Cleanup
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://test-region-all-bucket
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://test-region-create-bucket
}

# Main test execution
main() {
    setup
    
    echo -e "\n${YELLOW}=== Running HeadBucket and Ownership Tests ===${NC}"
    
    test_head_bucket_region
    test_get_ownership_default
    test_put_ownership
    test_delete_ownership
    test_region_header_in_all_responses
    
    echo -e "\n${GREEN}=== All HeadBucket and Ownership tests passed! ===${NC}"
}

# Only run main if script is executed directly (not sourced)
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main
fi
