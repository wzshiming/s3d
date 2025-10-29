#!/bin/bash
# Common setup and configuration for e2e tests (DEPRECATED)
# This file is kept for backward compatibility
# New code should use lib/config.sh, lib/utils.sh, and lib/server.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load the new library files
source "${SCRIPT_DIR}/lib/config.sh"
source "${SCRIPT_DIR}/lib/utils.sh"
source "${SCRIPT_DIR}/lib/server.sh"

# Legacy variable mapping for backward compatibility
export SERVER_PORT="$E2E_SERVER_PORT"
export SERVER_ADDR="$E2E_SERVER_ADDR"
export TEST_BUCKET="$E2E_TEST_BUCKET"
export SERVER_PID="$E2E_SERVER_PID"

# Setup test data directories
export TEST_DATA_DIR=$(create_temp_dir)
export SERVER_DATA_DIR=$(create_temp_dir)
export E2E_TEST_DATA_DIR="$TEST_DATA_DIR"
export E2E_SERVER_DATA_DIR="$SERVER_DATA_DIR"

# Cleanup function (legacy)
cleanup() {
    cleanup_server
}

# Setup function (legacy)
setup() {
    echo -e "${YELLOW}Starting S3-compatible server e2e test...${NC}"
    echo "Server address: ${SERVER_ADDR}"
    echo "Test data directory: ${TEST_DATA_DIR}"
    echo "Server data directory: ${SERVER_DATA_DIR}"

    trap cleanup EXIT

    build_server "$E2E_BINARY_PATH" || exit 1
    verify_aws_cli
    
    # Start the server in the background without authentication
    echo -e "\n${YELLOW}Starting server (no authentication mode)...${NC}"
    "$E2E_BINARY_PATH" -addr ":${SERVER_PORT}" -data "${SERVER_DATA_DIR}" > /dev/null 2>&1 &
    SERVER_PID=$!
    E2E_SERVER_PID=$!
    echo "Server PID: ${SERVER_PID}"

    # Wait for server to start
    wait_for_server "${SERVER_ADDR}" || exit 1
}
