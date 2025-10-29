#!/bin/bash
# Server lifecycle management functions

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

# Start server without authentication
start_server() {
    local port="${1:-$E2E_SERVER_PORT}"
    local data_dir="${2:-$E2E_SERVER_DATA_DIR}"
    
    section_header "Starting S3-compatible server (no authentication)"
    echo "Server address: http://localhost:${port}"
    echo "Server data directory: ${data_dir}"
    
    build_server "$E2E_BINARY_PATH" || return 1
    
    # Start the server in the background
    "$E2E_BINARY_PATH" -addr ":${port}" -data "${data_dir}" > /dev/null 2>&1 &
    E2E_SERVER_PID=$!
    echo "Server PID: ${E2E_SERVER_PID}"
    
    # Wait for server to be ready
    wait_for_server "http://localhost:${port}" || return 1
    
    return 0
}

# Start server with authentication
start_auth_server() {
    local port="${1:-$E2E_AUTH_SERVER_PORT}"
    local data_dir="${2:-$E2E_AUTH_SERVER_DATA_DIR}"
    local access_key="${3:-$E2E_TEST_ACCESS_KEY}"
    local secret_key="${4:-$E2E_TEST_SECRET_KEY}"
    
    section_header "Starting S3-compatible server with authentication"
    echo "Server address: http://localhost:${port}"
    echo "Server data directory: ${data_dir}"
    echo "Access Key: ${access_key}"
    
    build_server "$E2E_BINARY_PATH" || return 1
    
    # Start the server with authentication
    "$E2E_BINARY_PATH" -addr ":${port}" -data "${data_dir}" -credentials "${access_key}:${secret_key}" > /dev/null 2>&1 &
    E2E_AUTH_SERVER_PID=$!
    echo "Server PID: ${E2E_AUTH_SERVER_PID}"
    
    # Wait for server to be ready
    wait_for_server "http://localhost:${port}" || return 1
    
    # Configure AWS CLI with test credentials
    export AWS_ACCESS_KEY_ID="${access_key}"
    export AWS_SECRET_ACCESS_KEY="${secret_key}"
    
    return 0
}

# Stop server
stop_server() {
    local pid="${1:-$E2E_SERVER_PID}"
    
    if [ -n "$pid" ]; then
        echo "Stopping server (PID: ${pid})..."
        kill -TERM "$pid" 2>/dev/null || true
        sleep 1
        kill -KILL "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
}

# Cleanup function for non-auth server
cleanup_server() {
    echo -e "\n${YELLOW}Cleaning up server...${NC}"
    stop_server "$E2E_SERVER_PID"
    
    if [ -n "$E2E_TEST_DATA_DIR" ] && [ -d "$E2E_TEST_DATA_DIR" ]; then
        rm -rf "${E2E_TEST_DATA_DIR}"
    fi
    
    if [ -n "$E2E_SERVER_DATA_DIR" ] && [ -d "$E2E_SERVER_DATA_DIR" ]; then
        rm -rf "${E2E_SERVER_DATA_DIR}"
    fi
}

# Cleanup function for auth server
cleanup_auth_server() {
    echo -e "\n${YELLOW}Cleaning up auth server...${NC}"
    stop_server "$E2E_AUTH_SERVER_PID"
    
    if [ -n "$E2E_AUTH_TEST_DATA_DIR" ] && [ -d "$E2E_AUTH_TEST_DATA_DIR" ]; then
        rm -rf "${E2E_AUTH_TEST_DATA_DIR}"
    fi
    
    if [ -n "$E2E_AUTH_SERVER_DATA_DIR" ] && [ -d "$E2E_AUTH_SERVER_DATA_DIR" ]; then
        rm -rf "${E2E_AUTH_SERVER_DATA_DIR}"
    fi
}
