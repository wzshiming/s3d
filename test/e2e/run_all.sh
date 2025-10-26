#!/bin/bash
set -e

# Main test runner for e2e tests
# Usage: ./run_all.sh [test_suite...]
# 
# Examples:
#   ./run_all.sh              # Run all tests
#   ./run_all.sh bucket       # Run only bucket tests
#   ./run_all.sh bucket object # Run bucket and object tests
#   ./run_all.sh auth         # Run authentication tests

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage
usage() {
    echo "Usage: $0 [test_suite...]"
    echo ""
    echo "Available test suites:"
    echo "  bucket    - Bucket operations tests"
    echo "  object    - Object operations tests"
    echo "  advanced  - Advanced features tests (multipart, sync, etc.)"
    echo "  auth      - Authentication tests"
    echo "  all       - Run all tests (default)"
    echo ""
    echo "Examples:"
    echo "  $0                # Run all tests"
    echo "  $0 bucket         # Run only bucket tests"
    echo "  $0 bucket object  # Run bucket and object tests"
    echo "  $0 auth           # Run authentication tests only"
}

# Check for help flag
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
    exit 0
fi

# Determine which tests to run
RUN_BUCKET=false
RUN_OBJECT=false
RUN_ADVANCED=false
RUN_AUTH=false

if [ $# -eq 0 ]; then
    # No arguments, run all tests
    RUN_BUCKET=true
    RUN_OBJECT=true
    RUN_ADVANCED=true
    RUN_AUTH=true
else
    # Parse arguments
    for arg in "$@"; do
        case "$arg" in
            bucket)
                RUN_BUCKET=true
                ;;
            object)
                RUN_OBJECT=true
                ;;
            advanced)
                RUN_ADVANCED=true
                ;;
            auth)
                RUN_AUTH=true
                ;;
            all)
                RUN_BUCKET=true
                RUN_OBJECT=true
                RUN_ADVANCED=true
                RUN_AUTH=true
                ;;
            *)
                echo -e "${RED}Unknown test suite: $arg${NC}"
                echo ""
                usage
                exit 1
                ;;
        esac
    done
fi

# Track overall success
OVERALL_SUCCESS=true

# Run non-auth tests (they share the same server instance)
if [ "$RUN_BUCKET" = true ] || [ "$RUN_OBJECT" = true ] || [ "$RUN_ADVANCED" = true ]; then
    source "${SCRIPT_DIR}/common.sh"
    
    # Source test files
    if [ "$RUN_BUCKET" = true ]; then
        source "${SCRIPT_DIR}/bucket_tests.sh"
    fi
    if [ "$RUN_OBJECT" = true ]; then
        source "${SCRIPT_DIR}/object_tests.sh"
    fi
    if [ "$RUN_ADVANCED" = true ]; then
        source "${SCRIPT_DIR}/advanced_tests.sh"
    fi
    
    # Run setup
    setup
    
    # Run bucket tests
    if [ "$RUN_BUCKET" = true ]; then
        echo -e "\n${YELLOW}========================================${NC}"
        echo -e "${YELLOW}Running Bucket Tests${NC}"
        echo -e "${YELLOW}========================================${NC}"
        
        test_list_empty_buckets
        test_create_bucket
        test_list_buckets
    fi
    
    # Run object tests
    if [ "$RUN_OBJECT" = true ]; then
        echo -e "\n${YELLOW}========================================${NC}"
        echo -e "${YELLOW}Running Object Tests${NC}"
        echo -e "${YELLOW}========================================${NC}"
        
        # Create bucket if not already created
        if [ "$RUN_BUCKET" != true ]; then
            aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET} 2>/dev/null || true
        fi
        
        test_upload_file
        test_list_objects
        test_download_file
        test_upload_multiple_files
        test_list_with_prefix
        test_copy_object
        test_rename_object
        test_delete_object
    fi
    
    # Run advanced tests
    if [ "$RUN_ADVANCED" = true ]; then
        echo -e "\n${YELLOW}========================================${NC}"
        echo -e "${YELLOW}Running Advanced Tests${NC}"
        echo -e "${YELLOW}========================================${NC}"
        
        # Create bucket if not already created
        if [ "$RUN_BUCKET" != true ] && [ "$RUN_OBJECT" != true ]; then
            aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 mb s3://${TEST_BUCKET} 2>/dev/null || true
        fi
        
        test_multipart_upload
        test_sync_directory
        test_upload_part_copy
    fi
    
    # Cleanup objects and buckets
    if [ "$RUN_OBJECT" = true ] || [ "$RUN_ADVANCED" = true ]; then
        test_remove_all_objects
    fi
    
    if [ "$RUN_BUCKET" = true ] || [ "$RUN_OBJECT" = true ] || [ "$RUN_ADVANCED" = true ]; then
        test_delete_bucket
        
        # Run nested path test (creates and deletes its own bucket) - only if advanced tests are enabled
        if [ "$RUN_ADVANCED" = true ]; then
            test_nested_paths
        fi
        
        # Clean up final test bucket
        aws --endpoint-url="${SERVER_ADDR}" --no-sign-request s3 rb s3://${TEST_BUCKET} --force 2>/dev/null || true
    fi
    
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}Non-auth tests passed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
fi

# Run authentication tests (separate server instance)
if [ "$RUN_AUTH" = true ]; then
    echo -e "\n${YELLOW}========================================${NC}"
    echo -e "${YELLOW}Running Authentication Tests${NC}"
    echo -e "${YELLOW}========================================${NC}"
    
    if ! "${SCRIPT_DIR}/auth_tests.sh"; then
        OVERALL_SUCCESS=false
        echo -e "\n${RED}========================================${NC}"
        echo -e "${RED}Authentication tests FAILED!${NC}"
        echo -e "${RED}========================================${NC}"
    fi
fi

# Final summary
if [ "$OVERALL_SUCCESS" = true ]; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}All requested tests passed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
else
    echo -e "\n${RED}========================================${NC}"
    echo -e "${RED}Some tests FAILED!${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi
