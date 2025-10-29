#!/bin/bash
# Configuration settings for e2e tests

# Server configuration
export E2E_SERVER_PORT="${E2E_SERVER_PORT:-9090}"
export E2E_SERVER_ADDR="http://localhost:${E2E_SERVER_PORT}"
export E2E_SERVER_PID=""

# Authentication server configuration
export E2E_AUTH_SERVER_PORT="${E2E_AUTH_SERVER_PORT:-9091}"
export E2E_AUTH_SERVER_ADDR="http://localhost:${E2E_AUTH_SERVER_PORT}"
export E2E_AUTH_SERVER_PID=""

# Test bucket names
export E2E_TEST_BUCKET="${E2E_TEST_BUCKET:-test-bucket-e2e}"
export E2E_AUTH_TEST_BUCKET="${E2E_AUTH_TEST_BUCKET:-test-auth-bucket-e2e}"

# Test credentials
export E2E_TEST_ACCESS_KEY="${E2E_TEST_ACCESS_KEY:-test-access-key-e2e}"
export E2E_TEST_SECRET_KEY="${E2E_TEST_SECRET_KEY:-test-secret-key-e2e}"

# AWS CLI configuration for non-auth tests
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Test data directories (will be set by test runner)
export E2E_TEST_DATA_DIR=""
export E2E_SERVER_DATA_DIR=""
export E2E_AUTH_TEST_DATA_DIR=""
export E2E_AUTH_SERVER_DATA_DIR=""

# Binary path
export E2E_BINARY_PATH="./s3d"
