#!/bin/bash
# Tests for multipart upload and advanced features

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# Test: Upload large file (multipart)
test_multipart_upload() {
    test_header "Upload large file (multipart)"
    # Create a 10MB file
    dd if=/dev/zero of="${TEST_DATA_DIR}/large-file.bin" bs=1M count=10 2>/dev/null
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/large-file.bin" "s3://${TEST_BUCKET}/large-file.bin"
    
    if object_exists "${SERVER_ADDR}" "${TEST_BUCKET}" "large-file.bin"; then
        assert_success "Large file uploaded"
    else
        assert_failure "Large file not found"
    fi
}

# Test: Sync directory
test_sync_directory() {
    test_header "Sync directory"
    mkdir -p "${TEST_DATA_DIR}/sync-test"
    for i in {1..3}; do
        echo "Sync test $i" > "${TEST_DATA_DIR}/sync-test/sync-${i}.txt"
    done
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 sync \
        "${TEST_DATA_DIR}/sync-test/" "s3://${TEST_BUCKET}/synced/"
    
    local synced_count=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls \
        "s3://${TEST_BUCKET}/synced/" | grep -c "sync-" || true)
    
    if [ "$synced_count" -eq 3 ]; then
        assert_success "Directory synced successfully"
    else
        assert_failure "Expected 3 synced files, found ${synced_count}"
    fi
}

# Test: UploadPartCopy - Copy existing object to multipart upload part
test_upload_part_copy() {
    test_header "UploadPartCopy (copy object to multipart part)"
    
    # Create bucket if it doesn't exist
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://${TEST_BUCKET}" 2>/dev/null || true
    
    # Create a source object to copy from
    echo "Source object content for UploadPartCopy test" > "${TEST_DATA_DIR}/source-for-copy.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/source-for-copy.txt" "s3://${TEST_BUCKET}/source-for-copy.txt"
    
    # Create part 1 data
    echo "Part 1 data" > "${TEST_DATA_DIR}/part1.txt"
    
    # Initiate multipart upload
    local upload_output=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api create-multipart-upload \
        --bucket "${TEST_BUCKET}" \
        --key multipart-with-copy.txt 2>&1)
    local upload_id=$(echo "$upload_output" | python3 -c "import sys, json; print(json.load(sys.stdin)['UploadId'])" 2>/dev/null || echo "")
    
    if [ -z "$upload_id" ]; then
        assert_failure "Failed to initiate multipart upload"
    fi
    
    echo "Upload ID: $upload_id"
    
    # Upload part 1 from local file
    local part1_output=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api upload-part \
        --bucket "${TEST_BUCKET}" \
        --key multipart-with-copy.txt \
        --part-number 1 \
        --upload-id "$upload_id" \
        --body "${TEST_DATA_DIR}/part1.txt" 2>&1)
    local part1_etag=$(echo "$part1_output" | python3 -c "import sys, json; print(json.load(sys.stdin)['ETag'])" 2>/dev/null || echo "")
    
    echo "Part 1 ETag: $part1_etag"
    
    # Upload part 2 using UploadPartCopy from the source object
    local part2_output=$(aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api upload-part-copy \
        --bucket "${TEST_BUCKET}" \
        --key multipart-with-copy.txt \
        --part-number 2 \
        --upload-id "$upload_id" \
        --copy-source "${TEST_BUCKET}/source-for-copy.txt" 2>&1)
    local part2_etag=$(echo "$part2_output" | python3 -c "import sys, json; print(json.load(sys.stdin)['CopyPartResult']['ETag'])" 2>/dev/null || echo "")
    
    if [ -z "$part2_etag" ]; then
        # Abort the upload
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api abort-multipart-upload \
            --bucket "${TEST_BUCKET}" \
            --key multipart-with-copy.txt \
            --upload-id "$upload_id" 2>/dev/null || true
        assert_failure "UploadPartCopy failed"
    fi
    
    echo "Part 2 ETag (from UploadPartCopy): $part2_etag"
    
    # Complete multipart upload with proper JSON formatting
    cat > "${TEST_DATA_DIR}/parts.json" <<EOF
{
    "Parts": [
        {
            "ETag": $part1_etag,
            "PartNumber": 1
        },
        {
            "ETag": $part2_etag,
            "PartNumber": 2
        }
    ]
}
EOF
    
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api complete-multipart-upload \
        --bucket "${TEST_BUCKET}" \
        --key multipart-with-copy.txt \
        --upload-id "$upload_id" \
        --multipart-upload "file://${TEST_DATA_DIR}/parts.json" 2>&1 > /dev/null
    
    # Verify the object exists
    if object_exists "${SERVER_ADDR}" "${TEST_BUCKET}" "multipart-with-copy.txt"; then
        assert_success "UploadPartCopy completed successfully"
    else
        assert_failure "Multipart upload with copy failed"
    fi
}

# Test: Test with nested paths
test_nested_paths() {
    test_header "Test with nested paths"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb "s3://${TEST_BUCKET}" 2>/dev/null || true
    echo "Nested content" > "${TEST_DATA_DIR}/nested.txt"
    aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 cp \
        "${TEST_DATA_DIR}/nested.txt" "s3://${TEST_BUCKET}/level1/level2/level3/nested.txt"
    
    # Check for the full path
    if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 ls "s3://${TEST_BUCKET}/level1/level2/level3/" | grep -q "nested.txt"; then
        assert_success "Nested path upload successful"
    else
        assert_failure "Nested path upload failed"
    fi
}
