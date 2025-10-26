#!/bin/bash
# S3 Compatibility Test Runner
# This script runs the Ceph s3-tests suite against the s3d server

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
S3TESTS_DIR="${SCRIPT_DIR}/s3-tests"
RESULTS_FILE="${SCRIPT_DIR}/compatibility_results.txt"
SUMMARY_FILE="${SCRIPT_DIR}/compatibility_summary.md"
S3D_PID=""
S3D_PORT=""
VERBOSE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose    Enable verbose output"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  S3D_ENDPOINT     Use existing s3d server (e.g., http://localhost:8080)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Cleanup function
cleanup() {
    if [ -n "$S3D_PID" ]; then
        echo -e "\n${YELLOW}Stopping s3d server (PID: $S3D_PID)...${NC}"
        kill $S3D_PID 2>/dev/null || true
        wait $S3D_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Start s3d server if not already running
start_s3d() {
    if [ -n "$S3D_ENDPOINT" ]; then
        echo -e "${BLUE}Using existing s3d server at $S3D_ENDPOINT${NC}"
        S3D_PORT=$(echo $S3D_ENDPOINT | sed -n 's/.*:\([0-9]*\).*/\1/p')
        if [ -z "$S3D_PORT" ]; then
            S3D_PORT=80
        fi
        return
    fi

    echo -e "${BLUE}Building s3d server...${NC}"
    cd "$PROJECT_ROOT"
    go build -o "${SCRIPT_DIR}/s3d" ./cmd/s3d

    # Create temporary directory for s3d storage
    S3D_DATA_DIR=$(mktemp -d)
    echo -e "${BLUE}Created temporary storage directory: $S3D_DATA_DIR${NC}"

    # Find available port
    S3D_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
    
    echo -e "${BLUE}Starting s3d server on port $S3D_PORT...${NC}"
    "${SCRIPT_DIR}/s3d" -addr "127.0.0.1:$S3D_PORT" -data "$S3D_DATA_DIR" > "${SCRIPT_DIR}/s3d.log" 2>&1 &
    S3D_PID=$!

    # Wait for server to start
    echo -e "${BLUE}Waiting for s3d server to start...${NC}"
    for i in {1..30}; do
        if curl -s "http://127.0.0.1:$S3D_PORT/" > /dev/null 2>&1; then
            echo -e "${GREEN}s3d server started successfully${NC}"
            return
        fi
        sleep 0.5
    done

    echo -e "${RED}Failed to start s3d server${NC}"
    cat "${SCRIPT_DIR}/s3d.log"
    exit 1
}

# Setup s3-tests
setup_s3tests() {
    echo -e "${BLUE}Setting up Ceph s3-tests...${NC}"
    
    if [ ! -d "$S3TESTS_DIR" ]; then
        echo -e "${BLUE}Cloning s3-tests repository...${NC}"
        cd "$SCRIPT_DIR"
        git clone --depth 1 https://github.com/ceph/s3-tests.git
    fi

    cd "$S3TESTS_DIR"
    
    # Create configuration file
    echo -e "${BLUE}Creating s3tests configuration...${NC}"
    cat > s3tests.conf <<EOF
[DEFAULT]
host = 127.0.0.1
port = $S3D_PORT
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3dtest-{random}-

[s3 main]
display_name = Main User
user_id = mainuser
email = main@example.com
api_name = default
access_key = TEST_ACCESS_KEY_MAIN_USER
secret_key = TEST_SECRET_KEY_MAIN_USER_FOR_S3D_COMPAT_TESTS

[s3 alt]
display_name = Alt User
user_id = altuser
email = alt@example.com
access_key = TEST_ACCESS_KEY_ALT_USER
secret_key = TEST_SECRET_KEY_ALT_USER_FOR_S3D_TESTS

[s3 tenant]
display_name = Tenant User
user_id = tenantuser
email = tenant@example.com
access_key = TEST_ACCESS_KEY_TENANT_USER
secret_key = TEST_SECRET_KEY_TENANT_USER_FOR_TESTS
tenant = testtenant
EOF

    # Install dependencies if needed
    if ! command -v tox &> /dev/null; then
        echo -e "${YELLOW}Installing tox...${NC}"
        pip3 install --user tox
    fi
}

# Run tests and capture results
run_tests() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}Running S3 Compatibility Tests${NC}"
    echo -e "${BLUE}========================================${NC}\n"

    cd "$S3TESTS_DIR"
    
    # Run tests and capture output
    local test_output=$(mktemp)
    
    if [ "$VERBOSE" = true ]; then
        S3TEST_CONF=s3tests.conf tox -- s3tests/functional/test_s3.py -v 2>&1 | tee "$test_output"
    else
        S3TEST_CONF=s3tests.conf tox -- s3tests/functional/test_s3.py 2>&1 | tee "$test_output"
    fi
    
    local exit_code=${PIPESTATUS[0]}
    
    # Parse results
    parse_results "$test_output"
    
    rm -f "$test_output"
    
    return $exit_code
}

# Parse test results and generate report
parse_results() {
    local test_output=$1
    
    echo -e "\n${BLUE}Generating compatibility report...${NC}"
    
    # Extract test results
    local passed=$(grep -c "PASSED" "$test_output" || echo "0")
    local failed=$(grep -c "FAILED" "$test_output" || echo "0")
    local skipped=$(grep -c "SKIPPED" "$test_output" || echo "0")
    local errors=$(grep -c "ERROR" "$test_output" || echo "0")
    local total=$((passed + failed + skipped + errors))
    
    # Calculate compatibility percentage
    local compatibility=0
    if [ $total -gt 0 ]; then
        compatibility=$((passed * 100 / total))
    fi
    
    # Generate summary report
    cat > "$SUMMARY_FILE" <<EOF
# S3 Compatibility Test Results

**Date:** $(date '+%Y-%m-%d %H:%M:%S')
**Server:** s3d
**Test Suite:** Ceph s3-tests

## Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| Total Tests | $total | 100% |
| Passed | $passed | $((total > 0 ? passed * 100 / total : 0))% |
| Failed | $failed | $((total > 0 ? failed * 100 / total : 0))% |
| Skipped | $skipped | $((total > 0 ? skipped * 100 / total : 0))% |
| Errors | $errors | $((total > 0 ? errors * 100 / total : 0))% |

**Overall Compatibility: ${compatibility}%**

## Test Categories

### Supported Features (Passed Tests)

The following S3 features are verified to be compatible:

EOF

    # Extract passed test names and categorize them
    grep "PASSED" "$test_output" | sed 's/.*::\(test_[^ ]*\).*/- \1/' | sort -u >> "$SUMMARY_FILE" || true
    
    cat >> "$SUMMARY_FILE" <<EOF

### Unsupported/Failing Features

The following S3 features are not compatible or have issues:

EOF

    # Extract failed test names
    grep "FAILED" "$test_output" | sed 's/.*::\(test_[^ ]*\).*/- \1/' | sort -u >> "$SUMMARY_FILE" || true
    
    cat >> "$SUMMARY_FILE" <<EOF

### Skipped Tests

EOF

    # Extract skipped test names
    grep "SKIPPED" "$test_output" | sed 's/.*::\(test_[^ ]*\).*/- \1/' | sort -u >> "$SUMMARY_FILE" || true
    
    cat >> "$SUMMARY_FILE" <<EOF

## Known Limitations

Based on the s3d README, the following features are not yet implemented:
- Bucket versioning
- Object locking
- Bucket policies
- ACLs
- Server-side encryption
- Object tagging
- Lifecycle policies

## Detailed Results

For detailed test output, see: \`compatibility_results.txt\`

## AWS S3 API Reference

- [AWS S3 API Operations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html)
- [Ceph s3-tests Repository](https://github.com/ceph/s3-tests)
EOF

    # Copy detailed results
    cp "$test_output" "$RESULTS_FILE"
    
    # Display summary
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}Test Results Summary${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "Total Tests:  ${BLUE}$total${NC}"
    echo -e "Passed:       ${GREEN}$passed${NC}"
    echo -e "Failed:       ${RED}$failed${NC}"
    echo -e "Skipped:      ${YELLOW}$skipped${NC}"
    echo -e "Errors:       ${RED}$errors${NC}"
    echo -e "\n${GREEN}Overall Compatibility: ${compatibility}%${NC}\n"
    echo -e "Detailed results saved to:"
    echo -e "  - ${BLUE}$RESULTS_FILE${NC}"
    echo -e "  - ${BLUE}$SUMMARY_FILE${NC}\n"
}

# Main execution
main() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}S3 Compatibility Test Runner${NC}"
    echo -e "${GREEN}========================================${NC}\n"
    
    start_s3d
    setup_s3tests
    run_tests
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Compatibility testing complete!${NC}"
    echo -e "${GREEN}========================================${NC}\n"
}

main
