#!/bin/bash

# E2E tests for non-ASCII object keys
# Tests object operations with non-ASCII characters using AWS CLI

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

# Test bucket name
BUCKET="test-nonascii-$(date +%s)"

echo "=== Testing Non-ASCII Object Keys ==="

# Create bucket
echo "Creating bucket: $BUCKET"
aws s3api create-bucket --bucket "$BUCKET" --endpoint-url "$ENDPOINT_URL" --region "$REGION" > /dev/null 2>&1

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    # List and delete all objects
    aws s3 rm "s3://$BUCKET" --recursive --endpoint-url "$ENDPOINT_URL" --region "$REGION" > /dev/null 2>&1 || true
    # Delete bucket
    aws s3api delete-bucket --bucket "$BUCKET" --endpoint-url "$ENDPOINT_URL" --region "$REGION" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# Test 1: Put object with Chinese characters
echo "Test 1: Put object with Chinese characters"
KEY1="文件.txt"
echo "Test content 1" | aws s3 cp - "s3://$BUCKET/$KEY1" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Put object with Chinese key"

# Test 2: Put object with Cyrillic characters
echo "Test 2: Put object with Cyrillic characters"
KEY2="файл.txt"
echo "Test content 2" | aws s3 cp - "s3://$BUCKET/$KEY2" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Put object with Cyrillic key"

# Test 3: Put object with Japanese characters
echo "Test 3: Put object with Japanese characters"
KEY3="ファイル.txt"
echo "Test content 3" | aws s3 cp - "s3://$BUCKET/$KEY3" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Put object with Japanese key"

# Test 4: Put object with emoji
echo "Test 4: Put object with emoji"
KEY4="file-😀.txt"
echo "Test content 4" | aws s3 cp - "s3://$BUCKET/$KEY4" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Put object with emoji key"

# Test 5: Put object with non-ASCII path
echo "Test 5: Put object with non-ASCII path"
KEY5="文件夹/文件.txt"
echo "Test content 5" | aws s3 cp - "s3://$BUCKET/$KEY5" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Put object with non-ASCII path"

# Test 6: List objects and verify all non-ASCII keys are present
echo "Test 6: List objects with non-ASCII keys"
OBJECTS=$(aws s3api list-objects-v2 --bucket "$BUCKET" --endpoint-url "$ENDPOINT_URL" --region "$REGION" --query 'Contents[].Key' --output text)
check_success "List objects"

# Check if all keys are present in the listing
for KEY in "$KEY1" "$KEY2" "$KEY3" "$KEY4" "$KEY5"; do
    if echo "$OBJECTS" | grep -q "$KEY"; then
        echo "  ✓ Found key: $KEY"
    else
        echo "  ✗ Key not found in listing: $KEY"
        exit 1
    fi
done

# Test 7: Get object with non-ASCII key
echo "Test 7: Get object with non-ASCII key"
CONTENT=$(aws s3 cp "s3://$BUCKET/$KEY1" - --endpoint-url "$ENDPOINT_URL" --region "$REGION")
check_success "Get object with Chinese key"
if [ "$CONTENT" = "Test content 1" ]; then
    echo "  ✓ Content matches"
else
    echo "  ✗ Content mismatch: got '$CONTENT', expected 'Test content 1'"
    exit 1
fi

# Test 8: Copy object with non-ASCII keys
echo "Test 8: Copy object with non-ASCII keys"
SRC_KEY="源文件.txt"
DST_KEY="目标文件.txt"
echo "Source content" | aws s3 cp - "s3://$BUCKET/$SRC_KEY" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
aws s3 cp "s3://$BUCKET/$SRC_KEY" "s3://$BUCKET/$DST_KEY" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Copy object with non-ASCII keys"

# Verify copied content
COPIED_CONTENT=$(aws s3 cp "s3://$BUCKET/$DST_KEY" - --endpoint-url "$ENDPOINT_URL" --region "$REGION")
if [ "$COPIED_CONTENT" = "Source content" ]; then
    echo "  ✓ Copied content matches"
else
    echo "  ✗ Copied content mismatch"
    exit 1
fi

# Test 9: Delete object with non-ASCII key
echo "Test 9: Delete object with non-ASCII key"
aws s3 rm "s3://$BUCKET/$KEY1" --endpoint-url "$ENDPOINT_URL" --region "$REGION"
check_success "Delete object with Chinese key"

# Verify object is deleted
if aws s3api head-object --bucket "$BUCKET" --key "$KEY1" --endpoint-url "$ENDPOINT_URL" --region "$REGION" > /dev/null 2>&1; then
    echo "  ✗ Object should have been deleted"
    exit 1
else
    echo "  ✓ Object successfully deleted"
fi

echo "=== All Non-ASCII Object Key Tests Passed ==="
