#!/bin/bash
# Tests for non-ASCII object keys

source "$(dirname "$0")/common.sh"

# Test: Non-ASCII object keys full CRUD cycle
test_nonascii_object_keys() {
    echo -e "\n${YELLOW}=== Testing Non-ASCII Object Keys ===${NC}"
    
    # Test 1: Put object with Chinese characters
    echo -e "\n${YELLOW}Test: Put object with Chinese characters${NC}"
    KEY1="文件.txt"
    echo "Test content 1" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - "s3://${TEST_BUCKET}/$KEY1"
    echo -e "${GREEN}✓ Put object with Chinese key${NC}"

    # Test 2: Put object with Cyrillic characters
    echo -e "\n${YELLOW}Test: Put object with Cyrillic characters${NC}"
    KEY2="файл.txt"
    echo "Test content 2" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - "s3://${TEST_BUCKET}/$KEY2"
    echo -e "${GREEN}✓ Put object with Cyrillic key${NC}"

    # Test 3: Put object with Japanese characters
    echo -e "\n${YELLOW}Test: Put object with Japanese characters${NC}"
    KEY3="ファイル.txt"
    echo "Test content 3" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - "s3://${TEST_BUCKET}/$KEY3"
    echo -e "${GREEN}✓ Put object with Japanese key${NC}"

    # Test 4: Put object with emoji
    echo -e "\n${YELLOW}Test: Put object with emoji${NC}"
    KEY4="file-😀.txt"
    echo "Test content 4" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - "s3://${TEST_BUCKET}/$KEY4"
    echo -e "${GREEN}✓ Put object with emoji key${NC}"

    # Test 5: Put object with non-ASCII path
    echo -e "\n${YELLOW}Test: Put object with non-ASCII path${NC}"
    KEY5="文件夹/文件.txt"
    echo "Test content 5" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - "s3://${TEST_BUCKET}/$KEY5"
    echo -e "${GREEN}✓ Put object with non-ASCII path${NC}"

    # Test 6: List objects and verify all non-ASCII keys are present
    echo -e "\n${YELLOW}Test: List objects with non-ASCII keys${NC}"
    OBJECTS=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api list-objects-v2 --bucket "${TEST_BUCKET}" --query 'Contents[].Key' --output text)

    # Check if all keys are present in the listing
    for KEY in "$KEY1" "$KEY2" "$KEY3" "$KEY4" "$KEY5"; do
        if echo "$OBJECTS" | grep -q "$KEY"; then
            echo -e "  ${GREEN}✓ Found key: $KEY${NC}"
        else
            echo -e "  ${RED}✗ Key not found in listing: $KEY${NC}"
            exit 1
        fi
    done

    # Test 7: Get object with non-ASCII key
    echo -e "\n${YELLOW}Test: Get object with non-ASCII key${NC}"
    CONTENT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "s3://${TEST_BUCKET}/$KEY1" -)
    if [ "$CONTENT" = "Test content 1" ]; then
        echo -e "${GREEN}✓ Content matches${NC}"
    else
        echo -e "${RED}✗ Content mismatch: got '$CONTENT', expected 'Test content 1'${NC}"
        exit 1
    fi

    # Test 8: Copy object with non-ASCII keys
    echo -e "\n${YELLOW}Test: Copy object with non-ASCII keys${NC}"
    SRC_KEY="源文件.txt"
    DST_KEY="目标文件.txt"
    echo "Source content" | aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp - "s3://${TEST_BUCKET}/$SRC_KEY"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "s3://${TEST_BUCKET}/$SRC_KEY" "s3://${TEST_BUCKET}/$DST_KEY"
    echo -e "${GREEN}✓ Copy object with non-ASCII keys${NC}"

    # Verify copied content
    COPIED_CONTENT=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp "s3://${TEST_BUCKET}/$DST_KEY" -)
    if [ "$COPIED_CONTENT" = "Source content" ]; then
        echo -e "${GREEN}✓ Copied content matches${NC}"
    else
        echo -e "${RED}✗ Copied content mismatch${NC}"
        exit 1
    fi

    # Test 9: Delete object with non-ASCII key
    echo -e "\n${YELLOW}Test: Delete object with non-ASCII key${NC}"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rm "s3://${TEST_BUCKET}/$KEY1"
    echo -e "${GREEN}✓ Delete object with Chinese key${NC}"

    # Verify object is deleted
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object --bucket "${TEST_BUCKET}" --key "$KEY1" > /dev/null 2>&1; then
        echo -e "${RED}✗ Object should have been deleted${NC}"
        exit 1
    else
        echo -e "${GREEN}✓ Object successfully deleted${NC}"
    fi

    echo -e "\n${GREEN}=== All Non-ASCII Object Key Tests Passed ===${NC}"
}
