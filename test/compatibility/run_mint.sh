#!/bin/bash
set -e

# Compatibility test script using minio/mint
# This script starts an s3d server and runs the minio/mint compatibility tests against it

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SERVER_PORT=${SERVER_PORT:-9000}
SERVER_ENDPOINT="localhost:${SERVER_PORT}"
ACCESS_KEY=${ACCESS_KEY:-minioadmin}
SECRET_KEY=${SECRET_KEY:-minioadmin}
MINT_MODE=${MINT_MODE:-core}
DATA_DIR=$(mktemp -d)
LOG_DIR="${SCRIPT_DIR}/logs"
CONTAINER_NAME="s3d-mint-test-$$"

# Check if docker or podman is available
if command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
else
    echo -e "${RED}Error: Neither docker nor podman is installed${NC}"
    echo "Please install either docker or podman to run compatibility tests"
    exit 1
fi

# Build the s3d binary
echo -e "${BLUE}Building s3d server...${NC}"
cd "${PROJECT_ROOT}"
go build -o s3d ./cmd/s3d
echo -e "${GREEN}✓ Build complete${NC}"

# Start the s3d server in background
echo -e "${BLUE}Starting s3d server on port ${SERVER_PORT}...${NC}"
./s3d -addr=":${SERVER_PORT}" -data="${DATA_DIR}" -credentials="${ACCESS_KEY}:${SECRET_KEY}" > /tmp/s3d.log 2>&1 &
SERVER_PID=$!

# Cleanup function
cleanup() {
    echo -e "\n${BLUE}Cleaning up...${NC}"
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    rm -rf "${DATA_DIR}"
    ${CONTAINER_CMD} rm -f "${CONTAINER_NAME}" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
echo -e "${BLUE}Waiting for server to be ready...${NC}"
for i in {1..30}; do
    if curl -s "http://${SERVER_ENDPOINT}/" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Server is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Error: Server failed to start${NC}"
        echo "Server log:"
        cat /tmp/s3d.log
        exit 1
    fi
    sleep 1
done

# Create log directory
mkdir -p "${LOG_DIR}"

# Run mint tests
echo -e "${BLUE}Running minio/mint compatibility tests...${NC}"
echo -e "${YELLOW}MINT_MODE: ${MINT_MODE}${NC}"
echo -e "${YELLOW}SERVER_ENDPOINT: ${SERVER_ENDPOINT}${NC}\n"

# For local testing, we need to use host networking or get the host IP
# Using host.docker.internal for Docker Desktop, or host IP for others
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    SERVER_ENDPOINT="host.docker.internal:${SERVER_PORT}"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux - use host.containers.internal for podman, or host IP
    if [ "$CONTAINER_CMD" = "podman" ]; then
        SERVER_ENDPOINT="host.containers.internal:${SERVER_PORT}"
    else
        # For docker on Linux, we need to use the actual host IP
        HOST_IP=$(hostname -I | awk '{print $1}')
        SERVER_ENDPOINT="${HOST_IP}:${SERVER_PORT}"
    fi
fi

# Run mint container
${CONTAINER_CMD} run --name "${CONTAINER_NAME}" \
    --add-host=host.containers.internal:host-gateway \
    --add-host=host.docker.internal:host-gateway \
    -e "SERVER_ENDPOINT=${SERVER_ENDPOINT}" \
    -e "ACCESS_KEY=${ACCESS_KEY}" \
    -e "SECRET_KEY=${SECRET_KEY}" \
    -e "ENABLE_HTTPS=0" \
    -e "MINT_MODE=${MINT_MODE}" \
    minio/mint || true

# Copy logs from container
echo -e "\n${BLUE}Copying test results...${NC}"
${CONTAINER_CMD} cp "${CONTAINER_NAME}:/mint/log/log.json" "${LOG_DIR}/log.json" 2>/dev/null || {
    echo -e "${YELLOW}Warning: Could not copy log.json, container may have failed to start${NC}"
}

# Parse and display results
if [ -f "${LOG_DIR}/log.json" ]; then
    echo -e "\n${BLUE}Generating compatibility report...${NC}"
    "${SCRIPT_DIR}/parse_results.sh" "${LOG_DIR}/log.json"
else
    echo -e "${RED}Error: Test results not found${NC}"
    exit 1
fi

echo -e "\n${GREEN}Compatibility tests complete!${NC}"
echo -e "Full logs available at: ${LOG_DIR}/log.json"
