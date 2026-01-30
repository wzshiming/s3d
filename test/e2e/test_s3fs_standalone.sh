#!/bin/bash
# Standalone test script for s3fs-fuse binary compatibility
# This can be run independently to test s3fs-fuse with s3d

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}s3fs-fuse Binary Compatibility Test${NC}"
echo "======================================"
echo ""

# Check for s3fs
if ! command -v s3fs &> /dev/null; then
    echo -e "${YELLOW}s3fs-fuse is not installed.${NC}"
    echo ""
    echo "To install s3fs-fuse:"
    echo "  Ubuntu/Debian: sudo apt-get install s3fs"
    echo "  CentOS/RHEL:   sudo yum install s3fs-fuse"
    echo "  macOS:         brew install s3fs"
    echo ""
    echo "Or build from source: https://github.com/s3fs-fuse/s3fs-fuse"
    echo ""
    exit 1
fi

echo "s3fs-fuse found: $(which s3fs)"
s3fs --version 2>&1 | head -1

# Configuration
SERVER_PORT=${SERVER_PORT:-8080}
SERVER_ADDR="http://localhost:${SERVER_PORT}"
TEST_BUCKET="s3fs-test-bucket"
TEST_DATA_DIR=$(mktemp -d)
SERVER_DATA_DIR=$(mktemp -d)
MOUNT_POINT=$(mktemp -d)

echo ""
echo "Configuration:"
echo "  Server:      ${SERVER_ADDR}"
echo "  Bucket:      ${TEST_BUCKET}"
echo "  Mount point: ${MOUNT_POINT}"
echo "  Data dir:    ${SERVER_DATA_DIR}"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    
    # Unmount s3fs
    if mountpoint -q ${MOUNT_POINT} 2>/dev/null; then
        fusermount -u ${MOUNT_POINT} 2>/dev/null || umount ${MOUNT_POINT} 2>/dev/null || true
    fi
    
    # Kill server
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    
    # Cleanup directories
    rm -rf "${TEST_DATA_DIR}"
    rm -rf "${SERVER_DATA_DIR}"
    rmdir "${MOUNT_POINT}" 2>/dev/null || true
    rm -f "${PASSWD_FILE}" 2>/dev/null || true
}
trap cleanup EXIT

# Build the server
echo -e "${YELLOW}Building s3d server...${NC}"
go build -o ./s3d-test ./cmd/s3d
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to build server${NC}"
    exit 1
fi
echo -e "${GREEN}Server built successfully${NC}"

# Start the server
echo -e "${YELLOW}Starting s3d server...${NC}"
./s3d-test -addr ":${SERVER_PORT}" -data "${SERVER_DATA_DIR}" > /tmp/s3d-test.log 2>&1 &
SERVER_PID=$!
echo "Server PID: ${SERVER_PID}"

# Wait for server to be ready
echo "Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s "${SERVER_ADDR}" > /dev/null 2>&1; then
        echo -e "${GREEN}Server is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Server failed to start${NC}"
        exit 1
    fi
    sleep 1
done

# Configure AWS CLI
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Create bucket
echo ""
echo -e "${YELLOW}Creating test bucket...${NC}"
aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET}
echo -e "${GREEN}Bucket created${NC}"

# Create s3fs credentials file
PASSWD_FILE=$(mktemp)
echo "test:test" > "${PASSWD_FILE}"
chmod 600 "${PASSWD_FILE}"

# Mount the bucket with s3fs
echo ""
echo -e "${YELLOW}Mounting bucket with s3fs...${NC}"
s3fs ${TEST_BUCKET} ${MOUNT_POINT} \
    -o passwd_file=${PASSWD_FILE} \
    -o url=${SERVER_ADDR} \
    -o use_path_request_style \
    -o dbglevel=info \
    -f -o no_check_certificate \
    -o enable_noobj_cache &

S3FS_PID=$!
sleep 3

# Check if mount succeeded
if ! mountpoint -q ${MOUNT_POINT} 2>/dev/null; then
    if [ ! -d "${MOUNT_POINT}" ] || [ "$(ls -A ${MOUNT_POINT} 2>/dev/null | wc -l)" -eq 0 ]; then
        echo -e "${RED}s3fs mount failed${NC}"
        echo "Check logs for details"
        exit 1
    fi
fi

echo -e "${GREEN}s3fs mounted successfully${NC}"
echo ""

# Run tests
echo -e "${YELLOW}Running s3fs-fuse compatibility tests...${NC}"
echo ""

# Test 1: Create directory
echo "Test 1: Create directory via s3fs..."
mkdir -p "${MOUNT_POINT}/testdir"
if [ -d "${MOUNT_POINT}/testdir" ]; then
    echo -e "${GREEN}✓ Directory created${NC}"
else
    echo -e "${RED}✗ Failed to create directory${NC}"
    exit 1
fi

# Test 2: Create file
echo "Test 2: Create file via s3fs..."
echo "Hello from s3fs!" > "${MOUNT_POINT}/testdir/hello.txt"
if [ -f "${MOUNT_POINT}/testdir/hello.txt" ]; then
    echo -e "${GREEN}✓ File created${NC}"
else
    echo -e "${RED}✗ Failed to create file${NC}"
    exit 1
fi

# Test 3: Read file
echo "Test 3: Read file via s3fs..."
CONTENT=$(cat "${MOUNT_POINT}/testdir/hello.txt")
if [ "$CONTENT" = "Hello from s3fs!" ]; then
    echo -e "${GREEN}✓ File content correct${NC}"
else
    echo -e "${RED}✗ File content mismatch${NC}"
    exit 1
fi

# Test 4: Verify via S3 API
echo "Test 4: Verify file via S3 API..."
aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api get-object \
    --bucket ${TEST_BUCKET} \
    --key "testdir/hello.txt" \
    "${TEST_DATA_DIR}/retrieved.txt" > /dev/null 2>&1

RETRIEVED_CONTENT=$(cat "${TEST_DATA_DIR}/retrieved.txt")
if [ "$RETRIEVED_CONTENT" = "Hello from s3fs!" ]; then
    echo -e "${GREEN}✓ File accessible via S3 API${NC}"
else
    echo -e "${RED}✗ S3 API content mismatch${NC}"
    exit 1
fi

# Test 5: List directory
echo "Test 5: List directory via s3fs..."
FILE_COUNT=$(ls "${MOUNT_POINT}/testdir" | wc -l)
if [ "$FILE_COUNT" -ge 1 ]; then
    echo -e "${GREEN}✓ Directory listing works (${FILE_COUNT} item(s))${NC}"
else
    echo -e "${RED}✗ Directory listing failed${NC}"
    exit 1
fi

# Test 6: Create nested directories
echo "Test 6: Create nested directories..."
mkdir -p "${MOUNT_POINT}/testdir/level1/level2/level3"
if [ -d "${MOUNT_POINT}/testdir/level1/level2/level3" ]; then
    echo -e "${GREEN}✓ Nested directories created${NC}"
else
    echo -e "${RED}✗ Failed to create nested directories${NC}"
    exit 1
fi

# Test 7: Delete file
echo "Test 7: Delete file via s3fs..."
rm -f "${MOUNT_POINT}/testdir/hello.txt"
if [ ! -f "${MOUNT_POINT}/testdir/hello.txt" ]; then
    echo -e "${GREEN}✓ File deleted${NC}"
else
    echo -e "${RED}✗ Failed to delete file${NC}"
    exit 1
fi

# Test 8: Verify deletion via S3 API
echo "Test 8: Verify deletion via S3 API..."
if aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3api head-object \
    --bucket ${TEST_BUCKET} \
    --key "testdir/hello.txt" > /dev/null 2>&1; then
    echo -e "${RED}✗ File still exists in S3${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Deletion confirmed via S3 API${NC}"
fi

echo ""
echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}All tests passed successfully!${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""
echo "s3d is fully compatible with s3fs-fuse!"
