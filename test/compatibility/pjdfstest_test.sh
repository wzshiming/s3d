#!/usr/bin/env bash
# Run pjd/pjdfstest POSIX compatibility tests against s3d via an s3fs-fuse mount

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
export ACCESS_KEY="${ACCESS_KEY:-test}"
export SECRET_KEY="${SECRET_KEY:-test}"
export TEST_BUCKET="${TEST_BUCKET:-pjdfstest}"

export PJDFSTEST_DIR="${REPO_ROOT}/test/compatibility/pjdfstest"
export PJDFSTEST_LOG_DIR="${REPO_ROOT}/test/compatibility/pjdfstest-logs"
export MOUNT_POINT="$(mktemp -d)"
export PASSWD_FILE="$(mktemp)"
export S3FS_MOUNTED=""

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    if [ -n "${S3FS_MOUNTED}" ]; then
        sudo fusermount -u "${MOUNT_POINT}" 2>/dev/null || sudo umount "${MOUNT_POINT}" 2>/dev/null || true
    fi
    if [ -n "${SERVER_PID}" ]; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    rm -f "${PASSWD_FILE}"
    rmdir "${MOUNT_POINT}" 2>/dev/null || true
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

    # Start the server with authentication
    echo -e "\n${YELLOW}Starting server with authentication...${NC}"
    ./s3d -addr "127.0.0.1:${SERVER_PORT}" -data "${SERVER_DATA_DIR}" -credentials "${ACCESS_KEY}:${SECRET_KEY}" > /dev/null 2>&1 &
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

    # Create the test bucket
    echo -e "\n${YELLOW}Creating test bucket '${TEST_BUCKET}'...${NC}"
    AWS_ACCESS_KEY_ID="${ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" \
        aws --endpoint-url "http://127.0.0.1:${SERVER_PORT}" s3 mb "s3://${TEST_BUCKET}" || {
        echo -e "${RED}Failed to create test bucket${NC}"
        exit 1
    }
}

# Install s3fs-fuse if not available
setup_s3fs() {
    echo -e "\n${YELLOW}Setting up s3fs-fuse...${NC}"

    if ! command -v s3fs &> /dev/null; then
        echo "s3fs not found, installing via apt..."
        sudo apt-get update -qq
        sudo apt-get install -y s3fs
    fi

    echo "s3fs-fuse binary found: $(which s3fs)"
    s3fs --version 2>&1 | head -1 || echo "s3fs version unknown"
}

# Clone and build pjdfstest
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

    cd "${PJDFSTEST_DIR}"
    if [ ! -x "./pjdfstest" ]; then
        echo "Building pjdfstest..."
        autoreconf -ifs
        ./configure
        make pjdfstest
    fi
    echo -e "${GREEN}pjdfstest built successfully${NC}"
}

# Mount the test bucket with s3fs
mount_s3fs() {
    echo -e "\n${YELLOW}Mounting bucket '${TEST_BUCKET}' with s3fs at ${MOUNT_POINT}...${NC}"

    echo "${ACCESS_KEY}:${SECRET_KEY}" > "${PASSWD_FILE}"
    chmod 600 "${PASSWD_FILE}"

    # Mount as root with allow_other so that the tests (which run as root and
    # switch to unprivileged users) can access the mount.
    sudo s3fs "${TEST_BUCKET}" "${MOUNT_POINT}" \
        -o passwd_file="${PASSWD_FILE}" \
        -o url="http://127.0.0.1:${SERVER_PORT}" \
        -o use_path_request_style \
        -o allow_other \
        -o umask=000

    # Wait for the mount to be ready
    for i in {1..30}; do
        if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
            S3FS_MOUNTED="1"
            echo -e "${GREEN}s3fs mount successful${NC}"
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${RED}s3fs mount failed${NC}"
            exit 1
        fi
        sleep 1
    done
}

# Run pjdfstest with prove
run_pjdfstest() {
    echo -e "\n${YELLOW}Running pjd/pjdfstest compatibility tests...${NC}"
    echo "Log directory: ${PJDFSTEST_LOG_DIR}"

    local workdir="${MOUNT_POINT}/pjdfstest-workdir"
    sudo mkdir -p "${workdir}"
    cd "${workdir}"

    # Run the whole test suite as root with prove; tests operate in the
    # current working directory, which is inside the s3fs mount.
    sudo prove -r "${PJDFSTEST_DIR}/tests" 2>&1 | tee "${PJDFSTEST_LOG_DIR}/test_output.log" || true

    cd "${REPO_ROOT}"

    echo -e "\n${YELLOW}pjdfstest completed${NC}"
}

# Parse and display results
parse_results() {
    local output_log="${PJDFSTEST_LOG_DIR}/test_output.log"

    if [ ! -f "${output_log}" ]; then
        echo -e "${RED}No test output log found at ${output_log}${NC}"
        return 1
    fi

    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}pjd/pjdfstest Results${NC}"
    echo -e "${YELLOW}========================================${NC}"

    grep -E '^(Files=|Result:)' "${output_log}" || true

    echo -e "\n${YELLOW}Full results saved to: ${PJDFSTEST_LOG_DIR}${NC}"
}

# Main execution
main() {
    setup
    setup_s3fs
    setup_pjdfstest
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
