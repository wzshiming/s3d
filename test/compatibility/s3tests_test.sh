#!/usr/bin/env bash
# Run ceph/s3-tests compatibility tests against s3d

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
# Main credentials (from ceph/s3-tests sample config)
export ACCESS_KEY="0555b35654ad1656d804"
export SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
# Alt credentials
export ALT_ACCESS_KEY="NOPQRSTUVWXYZABCDEFG"
export ALT_SECRET_KEY="nopqrstuvwxyzabcdefghijklmnabcdefghijklm"
# Tenant credentials
export TENANT_ACCESS_KEY="HIJKLMNOPQRSTUVWXYZA"
export TENANT_SECRET_KEY="opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab"
# IAM credentials
export IAM_ACCESS_KEY="ABCDEFGHIJKLMNOPQRST"
export IAM_SECRET_KEY="abcdefghijklmnopqrstuvwxyzabcdefghijklmn"
# IAM root credentials
export IAM_ROOT_ACCESS_KEY="AAAAAAAAAAAAAAAAAAaa"
export IAM_ROOT_SECRET_KEY="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
# IAM alt root credentials
export IAM_ALT_ROOT_ACCESS_KEY="BBBBBBBBBBBBBBBBBBbb"
export IAM_ALT_ROOT_SECRET_KEY="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

export S3TESTS_DIR="${REPO_ROOT}/test/compatibility/s3-tests"
export S3TESTS_LOG_DIR="${REPO_ROOT}/test/compatibility/s3tests-logs"
export S3TESTS_CONF="${S3TESTS_LOG_DIR}/s3tests.conf"

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
    echo -e "${YELLOW}Starting S3-compatible server for s3-tests...${NC}"
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
    # Multiple credentials can be passed separated by comma
    echo -e "\n${YELLOW}Starting server with authentication...${NC}"
    ALL_CREDENTIALS="${ACCESS_KEY}:${SECRET_KEY},${ALT_ACCESS_KEY}:${ALT_SECRET_KEY},${TENANT_ACCESS_KEY}:${TENANT_SECRET_KEY},${IAM_ACCESS_KEY}:${IAM_SECRET_KEY},${IAM_ROOT_ACCESS_KEY}:${IAM_ROOT_SECRET_KEY},${IAM_ALT_ROOT_ACCESS_KEY}:${IAM_ALT_ROOT_SECRET_KEY}"
    ./s3d -addr "0.0.0.0:${SERVER_PORT}" -data "${SERVER_DATA_DIR}" -credentials "${ALL_CREDENTIALS}" > /dev/null 2>&1 &
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

# Clone or update s3-tests repository
setup_s3tests() {
    echo -e "\n${YELLOW}Setting up ceph/s3-tests...${NC}"
    
    mkdir -p "${S3TESTS_LOG_DIR}"
    
    if [ -d "${S3TESTS_DIR}" ]; then
        echo "Updating existing s3-tests repository..."
        cd "${S3TESTS_DIR}"
        git pull --quiet || true
    else
        echo "Cloning s3-tests repository..."
        git clone --depth 1 https://github.com/ceph/s3-tests.git "${S3TESTS_DIR}"
    fi
    
    # Generate configuration file
    echo -e "\n${YELLOW}Generating s3tests configuration...${NC}"
    cat > "${S3TESTS_CONF}" << EOF
[DEFAULT]
host = localhost
port = ${SERVER_PORT}
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3d-test-{random}-
iam name prefix = s3d-tests-
iam path prefix = /s3d-tests/

[s3 main]
display_name = Main Tester
user_id = testid
email = tester@example.com
api_name = default
access_key = ${ACCESS_KEY}
secret_key = ${SECRET_KEY}

[s3 alt]
display_name = Alt User
email = altuser@example.com
user_id = altid
access_key = ${ALT_ACCESS_KEY}
secret_key = ${ALT_SECRET_KEY}

[s3 tenant]
display_name = Tenant User
user_id = tenantuser
email = tenant@example.com
access_key = ${TENANT_ACCESS_KEY}
secret_key = ${TENANT_SECRET_KEY}
tenant = testtenant

[iam]
display_name = IAM User
user_id = iamid
email = iam@example.com
access_key = ${IAM_ACCESS_KEY}
secret_key = ${IAM_SECRET_KEY}

[iam root]
access_key = ${IAM_ROOT_ACCESS_KEY}
secret_key = ${IAM_ROOT_SECRET_KEY}
user_id = rootid
email = root@example.com

[iam alt root]
access_key = ${IAM_ALT_ROOT_ACCESS_KEY}
secret_key = ${IAM_ALT_ROOT_SECRET_KEY}
user_id = altrootid
email = altroot@example.com
EOF
    
    echo -e "${GREEN}Configuration file created at ${S3TESTS_CONF}${NC}"
}

# Install s3-tests dependencies
install_dependencies() {
    echo -e "\n${YELLOW}Installing s3-tests dependencies...${NC}"
    cd "${S3TESTS_DIR}"
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        python3 -m venv venv
    fi
    
    # Activate and install dependencies
    source venv/bin/activate
    pip install --quiet --upgrade pip
    pip install --quiet -r requirements.txt
    
    echo -e "${GREEN}Dependencies installed${NC}"
}

# Run s3-tests
run_s3tests() {
    echo -e "\n${YELLOW}Running ceph/s3-tests compatibility tests...${NC}"
    echo "Log directory: ${S3TESTS_LOG_DIR}"
    
    cd "${S3TESTS_DIR}"
    source venv/bin/activate
    
    # Run tests with pytest and output JUnit XML format
    export S3TEST_CONF="${S3TESTS_CONF}"
    
    # Run only the functional tests, excluding tests that are known to require specific features
    # We use pytest markers to exclude tests that fail on non-AWS/non-Ceph implementations
    pytest s3tests/functional/test_s3.py \
        --junit-xml="${S3TESTS_LOG_DIR}/results.xml" \
        --tb=short \
        -v \
        -m "not fails_on_rgw and not fails_on_aws and not fails_on_dbstore and not fails_on_mod_proxy_fcgi and not fails_strict_rfc2616" \
        2>&1 | tee "${S3TESTS_LOG_DIR}/test_output.log" || true
    
    echo -e "\n${YELLOW}s3-tests completed${NC}"
}

# Parse and display results
parse_results() {
    local log_file="${S3TESTS_LOG_DIR}/results.xml"
    local output_log="${S3TESTS_LOG_DIR}/test_output.log"
    
    if [ ! -f "$log_file" ]; then
        echo -e "${RED}No test results file found at ${log_file}${NC}"
        return 1
    fi

    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}ceph/s3-tests Results${NC}"
    echo -e "${YELLOW}========================================${NC}"

    # Parse JUnit XML results
    local tests=$(grep -oP 'tests="\K[0-9]+' "$log_file" | head -1 || echo "0")
    local errors=$(grep -oP 'errors="\K[0-9]+' "$log_file" | head -1 || echo "0")
    local failures=$(grep -oP 'failures="\K[0-9]+' "$log_file" | head -1 || echo "0")
    local skipped=$(grep -oP 'skipped="\K[0-9]+' "$log_file" | head -1 || echo "0")
    local passed=$((tests - errors - failures - skipped))

    echo -e "\n${GREEN}PASSED: ${passed}${NC}"
    echo -e "${RED}FAILED: ${failures}${NC}"
    echo -e "${RED}ERRORS: ${errors}${NC}"
    echo -e "${YELLOW}SKIPPED: ${skipped}${NC}"
    echo -e "TOTAL: ${tests}"

    echo -e "\n${YELLOW}Full results saved to: ${S3TESTS_LOG_DIR}${NC}"
}

# Main execution
main() {
    setup
    setup_s3tests
    install_dependencies
    
    # Run s3-tests
    run_s3tests
    
    # Parse and display results
    parse_results
    
    echo -e "\n${GREEN}Compatibility tests completed!${NC}"
    echo -e "See ${SCRIPT_DIR}/s3tests_compatibility.md for known limitations."
    
    return 0
}

# Run if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
