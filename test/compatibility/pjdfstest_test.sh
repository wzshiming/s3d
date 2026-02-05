#!/usr/bin/env bash
# Run pjd/pjdfstest compatibility tests against s3d via s3fs-fuse

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/../..")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
export SERVER_PORT="${SERVER_PORT:-9000}"
export SERVER_DATA_DIR="$(mktemp -d)"
export SERVER_PID=""
export ACCESS_KEY="pjdfstest"
export SECRET_KEY="pjdfstestsecret"
export BUCKET_NAME="pjdfstest-bucket"

export PJDFSTEST_DIR="${REPO_ROOT}/test/compatibility/pjdfstest"
export PJDFSTEST_LOG_DIR="${REPO_ROOT}/test/compatibility/pjdfstest-logs"
export S3FS_MOUNT_DIR=""

# Enable user_allow_other in fuse.conf if not already enabled (for CI)
enable_fuse_allow_other() {
    if ! grep -q "^user_allow_other" /etc/fuse.conf 2>/dev/null; then
        echo -e "${YELLOW}Enabling user_allow_other in /etc/fuse.conf...${NC}"
        echo "user_allow_other" | sudo tee -a /etc/fuse.conf > /dev/null
    fi
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    
    # Unmount s3fs if mounted
    if [ -n "$S3FS_MOUNT_DIR" ] && mountpoint -q "$S3FS_MOUNT_DIR" 2>/dev/null; then
        echo "Unmounting s3fs..."
        sudo fusermount -u "$S3FS_MOUNT_DIR" 2>/dev/null || sudo umount "$S3FS_MOUNT_DIR" 2>/dev/null || true
        sleep 1
    fi
    
    # Remove mount directory
    if [ -n "$S3FS_MOUNT_DIR" ] && [ -d "$S3FS_MOUNT_DIR" ]; then
        rmdir "$S3FS_MOUNT_DIR" 2>/dev/null || true
    fi
    
    # Stop server
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    
    # Remove server data directory
    rm -rf "${SERVER_DATA_DIR}"
    # Keep log directory for results
}

# Setup function
setup() {
    echo -e "${YELLOW}Starting S3-compatible server for pjdfstest...${NC}"
    echo "Server port: ${SERVER_PORT}"
    echo "Server data directory: ${SERVER_DATA_DIR}"

    trap cleanup EXIT

    # Build the server
    echo -e "\n${YELLOW}Building server...${NC}"
    cd "${REPO_ROOT}"
    go build -o ./s3d ./cmd/s3d || {
        echo -e "${RED}Failed to build server${NC}"
        exit 1
    }
    echo -e "${GREEN}Server built successfully${NC}"

    # Start the server with authentication, binding to all interfaces
    echo -e "\n${YELLOW}Starting server with authentication...${NC}"
    ./s3d -addr "0.0.0.0:${SERVER_PORT}" -data "${SERVER_DATA_DIR}" -credentials "${ACCESS_KEY}:${SECRET_KEY}" > /dev/null 2>&1 &
    SERVER_PID=$!
    echo "Server PID: ${SERVER_PID}"

    # Wait for server to start
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s "http://127.0.0.1:${SERVER_PORT}" > /dev/null 2>&1; then
            echo -e "${GREEN}Server is ready${NC}"
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${RED}Server failed to start${NC}"
            exit 1
        fi
        sleep 1
    done
    
    # Create the test bucket using AWS CLI
    echo -e "\n${YELLOW}Creating test bucket...${NC}"
    AWS_ACCESS_KEY_ID="${ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" \
        aws --endpoint-url "http://127.0.0.1:${SERVER_PORT}" s3 mb "s3://${BUCKET_NAME}" 2>/dev/null || true
    echo -e "${GREEN}Test bucket created${NC}"
}

# Install s3fs-fuse
install_s3fs() {
    echo -e "\n${YELLOW}Installing s3fs-fuse...${NC}"
    
    if command -v s3fs &> /dev/null; then
        echo -e "${GREEN}s3fs is already installed${NC}"
        return 0
    fi
    
    # Install s3fs based on the package manager
    if command -v apt-get &> /dev/null; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq s3fs
    elif command -v yum &> /dev/null; then
        sudo yum install -y epel-release
        sudo yum install -y s3fs-fuse
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y s3fs-fuse
    elif command -v brew &> /dev/null; then
        brew install --cask macfuse 2>/dev/null || true
        brew install gromgit/fuse/s3fs-mac 2>/dev/null || true
    else
        echo -e "${RED}No supported package manager found to install s3fs${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}s3fs installed successfully${NC}"
}

# Mount s3fs
mount_s3fs() {
    echo -e "\n${YELLOW}Mounting S3 bucket with s3fs...${NC}"
    
    # Enable allow_other in fuse.conf for CI
    enable_fuse_allow_other
    
    S3FS_MOUNT_DIR="$(mktemp -d)"
    echo "Mount directory: ${S3FS_MOUNT_DIR}"
    
    # Create password file
    local passwd_file="$(mktemp)"
    echo "${ACCESS_KEY}:${SECRET_KEY}" > "$passwd_file"
    chmod 600 "$passwd_file"
    
    # Mount the bucket with allow_other to allow root access for pjdfstest
    s3fs "${BUCKET_NAME}" "${S3FS_MOUNT_DIR}" \
        -o passwd_file="$passwd_file" \
        -o url="http://127.0.0.1:${SERVER_PORT}" \
        -o use_path_request_style \
        -o no_check_certificate \
        -o allow_other \
        -o umask=0000 \
        -o mp_umask=0000 \
        -o enable_noobj_cache \
        -o dbglevel=warn
    
    # Verify mount
    if mountpoint -q "$S3FS_MOUNT_DIR"; then
        echo -e "${GREEN}S3 bucket mounted successfully at ${S3FS_MOUNT_DIR}${NC}"
    else
        echo -e "${RED}Failed to mount S3 bucket${NC}"
        rm "$passwd_file"
        exit 1
    fi
    
    rm "$passwd_file"
}

# Clone or update pjdfstest repository
setup_pjdfstest() {
    echo -e "\n${YELLOW}Setting up pjd/pjdfstest...${NC}"
    
    mkdir -p "${PJDFSTEST_LOG_DIR}"
    
    if [ -d "${PJDFSTEST_DIR}" ]; then
        echo "Updating existing pjdfstest repository..."
        cd "${PJDFSTEST_DIR}"
        git pull --quiet || true
    else
        echo "Cloning pjdfstest repository..."
        git clone --depth 1 https://github.com/pjd/pjdfstest.git "${PJDFSTEST_DIR}"
    fi
    
    echo -e "${GREEN}pjdfstest repository ready${NC}"
}

# Build pjdfstest
build_pjdfstest() {
    echo -e "\n${YELLOW}Building pjdfstest...${NC}"
    
    cd "${PJDFSTEST_DIR}"
    
    # Run autoreconf if needed
    if [ ! -f "configure" ]; then
        autoreconf -ifs
    fi
    
    # Configure and build
    ./configure --quiet
    make --quiet pjdfstest
    
    if [ -f "pjdfstest" ]; then
        echo -e "${GREEN}pjdfstest built successfully${NC}"
    else
        echo -e "${RED}Failed to build pjdfstest${NC}"
        exit 1
    fi
}

# Run pjdfstest
run_pjdfstest() {
    echo -e "\n${YELLOW}Running pjd/pjdfstest compatibility tests...${NC}"
    echo "Log directory: ${PJDFSTEST_LOG_DIR}"
    echo "Testing mount point: ${S3FS_MOUNT_DIR}"
    
    cd "${S3FS_MOUNT_DIR}"
    
    # Run tests with prove (TAP harness)
    # Use sudo since pjdfstest requires root for some tests
    # Continue on error to capture all results
    sudo prove -rv "${PJDFSTEST_DIR}/tests" 2>&1 | tee "${PJDFSTEST_LOG_DIR}/test_output.log" || true
    
    # Also run with --formatter to get TAP output
    sudo prove -v "${PJDFSTEST_DIR}/tests" 2>&1 | tee "${PJDFSTEST_LOG_DIR}/tap_output.log" || true
    
    echo -e "\n${YELLOW}pjdfstest completed${NC}"
}

# Parse and display results
parse_results() {
    local log_file="${PJDFSTEST_LOG_DIR}/test_output.log"
    
    if [ ! -f "$log_file" ]; then
        echo -e "${RED}No test results file found at ${log_file}${NC}"
        return 1
    fi

    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}pjd/pjdfstest Results${NC}"
    echo -e "${YELLOW}========================================${NC}"

    # Count results from TAP output
    local pass_count=$(grep -c '^\s*ok ' "$log_file" 2>/dev/null || echo "0")
    local fail_count=$(grep -c '^\s*not ok ' "$log_file" 2>/dev/null || echo "0")
    local skip_count=$(grep -c '# skip' "$log_file" 2>/dev/null || echo "0")

    echo -e "\n${GREEN}PASSED: ${pass_count}${NC}"
    echo -e "${RED}FAILED: ${fail_count}${NC}"
    echo -e "${YELLOW}SKIPPED: ${skip_count}${NC}"

    echo -e "\n${YELLOW}Full results saved to: ${PJDFSTEST_LOG_DIR}${NC}"
}

# Main execution
main() {
    setup
    install_s3fs
    setup_pjdfstest
    build_pjdfstest
    mount_s3fs
    
    # Run pjdfstest
    run_pjdfstest
    
    # Parse and display results
    parse_results
    
    echo -e "\n${GREEN}Compatibility tests completed!${NC}"
    echo -e "See ${SCRIPT_DIR}/pjdfstest_compatibility.md for known limitations."
    
    return 0
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
