#!/usr/bin/env bash
# Run minio/mint compatibility tests against s3d

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
export SERVER_DATA_DIR=$(mktemp -d)
export SERVER_PID=""
export ACCESS_KEY="minioadmin"
export SECRET_KEY="minioadmin"
export MINT_LOG_DIR="${REPO_ROOT}/test/compatibility/mint-logs"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "${SERVER_DATA_DIR}"
    # Keep log directory for results
}

# Setup function
setup() {
    echo -e "${YELLOW}Starting S3-compatible server for mint tests...${NC}"
    echo "Server port: ${SERVER_PORT}"
    echo "Server data directory: ${SERVER_DATA_DIR}"

    trap cleanup EXIT

    # Build the server
    echo -e "\n${YELLOW}Building server...${NC}"
    cd "${REPO_ROOT}"
    go build -o ./s3d ./cmd/s3d
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to build server${NC}"
        exit 1
    fi
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
}

# Run mint tests using Docker
mint_test() {
    echo -e "\n${YELLOW}Running mint compatibility tests...${NC}"
    echo "Log directory: ${MINT_LOG_DIR}"

    DOCKER_HOST_IP="host.docker.internal"

    # Run mint container
    docker run --rm \
        --add-host host.docker.internal:host-gateway \
        -e "SERVER_ENDPOINT=${DOCKER_HOST_IP}:${SERVER_PORT}" \
        -e "ACCESS_KEY=${ACCESS_KEY}" \
        -e "SECRET_KEY=${SECRET_KEY}" \
        -e "ENABLE_HTTPS=0" \
        -e "SERVER_REGION=us-east-1" \
        -e "MINT_MODE=core" \
        -v "${MINT_LOG_DIR}:/mint/log" \
        docker.io/minio/mint:latest

    MINT_EXIT_CODE=$?
    
    echo -e "\n${YELLOW}Mint tests completed with exit code: ${MINT_EXIT_CODE}${NC}"
    return 0  # Always return success - we expect some tests to fail
}

# Parse and display mint results
parse_results() {
    local log_file="${MINT_LOG_DIR}/log.json"
    
    if [ ! -f "$log_file" ]; then
        echo -e "${RED}No log file found at ${log_file}${NC}"
        return 1
    fi

    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Mint Test Results${NC}"
    echo -e "${YELLOW}========================================${NC}"

    # Count results
    local pass_count=$(grep -c '"status":"PASS"' "$log_file" 2>/dev/null || echo "0")
    local fail_count=$(grep -c '"status":"FAIL"' "$log_file" 2>/dev/null || echo "0")
    local na_count=$(grep -c '"status":"NA"' "$log_file" 2>/dev/null || echo "0")

    echo -e "\n${GREEN}PASSED: ${pass_count}${NC}"
    echo -e "${RED}FAILED: ${fail_count}${NC}"
    echo -e "${YELLOW}N/A: ${na_count}${NC}"

    # Show detailed results by SDK
    # Note: This SDK list matches the mint test suite SDKs
    echo -e "\n${YELLOW}Detailed Results by SDK:${NC}"
    for sdk in aws-sdk-go aws-sdk-java aws-sdk-java-v2 aws-sdk-php aws-sdk-ruby awscli healthcheck mc minio-go minio-java minio-js minio-py s3cmd s3select versioning; do
        local sdk_tests=$(jq -r --arg sdk "$sdk" '.[] | select(.name == $sdk) | "\(.status)|\(.function)"' "$log_file" 2>/dev/null || true)
        
        if [ -n "$sdk_tests" ]; then
            local sdk_pass=$(echo "$sdk_tests" | grep -c '^PASS|' 2>/dev/null || echo "0")
            local sdk_fail=$(echo "$sdk_tests" | grep -c '^FAIL|' 2>/dev/null || echo "0")
            
            if [ "$sdk_fail" = "0" ]; then
                echo -e "\n  ${GREEN}✓${NC} ${YELLOW}$sdk${NC} ($sdk_pass passed, $sdk_fail failed)"
            else
                echo -e "\n  ${RED}✗${NC} ${YELLOW}$sdk${NC} ($sdk_pass passed, $sdk_fail failed)"
            fi
            
            # Show each test entry - using here-string to avoid subshell
            while IFS='|' read -r status func; do
                if [ "$status" = "PASS" ]; then
                    echo -e "      ${GREEN}✓${NC} $func"
                elif [ "$status" = "FAIL" ]; then
                    echo -e "      ${RED}✗${NC} $func"
                else
                    echo -e "      ${YELLOW}?${NC} $func"
                fi
            done <<< "$sdk_tests"
        fi
    done

    # Copy log file to repository for reference
    cp "$log_file" "${SCRIPT_DIR}/mint_results.json" 2>/dev/null || true

    echo -e "\n${YELLOW}Full results saved to: ${SCRIPT_DIR}/mint_results.json${NC}"
}

# Main execution
main() {
    setup
    
    # Run mint tests
    mint_test
    
    # Parse and display results
    parse_results
    
    echo -e "\n${GREEN}Compatibility tests completed!${NC}"
    echo -e "See ${SCRIPT_DIR}/mint_compatibility.md for known limitations."
    
    return 0
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
