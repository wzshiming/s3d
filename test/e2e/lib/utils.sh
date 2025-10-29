#!/bin/bash
# Common utility functions for e2e tests

# Colors for output (only set if not already defined)
if [ -z "${RED}" ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color
fi

# Test assertion helpers
assert_success() {
    local message="$1"
    echo -e "${GREEN}✓ ${message}${NC}"
}

assert_failure() {
    local message="$1"
    echo -e "${RED}✗ ${message}${NC}"
    exit 1
}

test_header() {
    local message="$1"
    echo -e "\n${YELLOW}Test: ${message}${NC}"
}

section_header() {
    local message="$1"
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}${message}${NC}"
    echo -e "${YELLOW}========================================${NC}"
}

section_success() {
    local message="$1"
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}${message}${NC}"
    echo -e "${GREEN}========================================${NC}"
}

# Check if a command exists
command_exists() {
    command -v "$1" &> /dev/null
}

# Verify AWS CLI is installed
verify_aws_cli() {
    if ! command_exists aws; then
        echo -e "${RED}AWS CLI is not installed. Please install it first.${NC}"
        echo "Install with: pip install awscli"
        exit 1
    fi
    
    echo -e "\n${YELLOW}AWS CLI version:${NC}"
    aws --version
}

# Wait for server to be ready
wait_for_server() {
    local server_addr="$1"
    local max_attempts="${2:-30}"
    
    echo "Waiting for server to be ready..."
    for i in $(seq 1 $max_attempts); do
        if curl -s "${server_addr}" > /dev/null 2>&1; then
            echo -e "${GREEN}Server is ready${NC}"
            return 0
        fi
        if [ $i -eq $max_attempts ]; then
            echo -e "${RED}Server failed to start${NC}"
            return 1
        fi
        sleep 1
    done
}

# Build the s3d server
build_server() {
    local binary_path="${1:-./s3d}"
    
    if [ -f "$binary_path" ]; then
        return 0
    fi
    
    echo -e "\n${YELLOW}Building server...${NC}"
    go build -o "$binary_path" ./cmd/s3d
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build server${NC}"
        return 1
    fi
    echo -e "${GREEN}Server built successfully${NC}"
    return 0
}

# Create a temporary directory with restrictive permissions
create_temp_dir() {
    local dir=$(mktemp -d)
    chmod 700 "$dir"
    echo "$dir"
}

# Compare two files
files_match() {
    local file1="$1"
    local file2="$2"
    diff "$file1" "$file2" > /dev/null
}

# Check if object exists in bucket
object_exists() {
    local endpoint="$1"
    local bucket="$2"
    local key="$3"
    local sign_request="${4:-no}"
    
    local sign_flag="--no-sign-request"
    if [ "$sign_request" = "yes" ]; then
        sign_flag=""
    fi
    
    aws --endpoint-url="$endpoint" $sign_flag s3 ls "s3://${bucket}/" | grep -q "$key"
}

# Check if bucket exists
bucket_exists() {
    local endpoint="$1"
    local bucket="$2"
    local sign_request="${3:-no}"
    
    local sign_flag="--no-sign-request"
    if [ "$sign_request" = "yes" ]; then
        sign_flag=""
    fi
    
    aws --endpoint-url="$endpoint" $sign_flag s3 ls | grep -q "$bucket"
}
