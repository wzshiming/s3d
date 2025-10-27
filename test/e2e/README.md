# E2E Test Suite

This directory contains end-to-end tests for the s3d S3-compatible server.

## Structure

```
test/e2e/
├── lib/                    # Shared libraries
│   ├── config.sh          # Configuration settings
│   ├── server.sh          # Server lifecycle management
│   └── utils.sh           # Utility functions
├── suites/                # Test suites (new modular tests)
│   └── basic_object_tests.sh
├── run_all.sh             # Main test runner
├── runner.sh              # Alternative modular test runner
├── common.sh              # Legacy compatibility layer
├── bucket_tests.sh        # Bucket operation tests
├── object_tests.sh        # Object operation tests
├── advanced_tests.sh      # Advanced features (multipart, sync, etc.)
├── duplicate_write_tests.sh  # Duplicate write compatibility tests
└── auth_tests.sh          # Authentication tests
```

## Libraries

### lib/config.sh
Central configuration for all tests. Defines:
- Server ports and addresses
- Test bucket names
- Test credentials
- Environment variables

### lib/server.sh
Server lifecycle management functions:
- `start_server()` - Start server without authentication
- `start_auth_server()` - Start server with authentication
- `stop_server()` - Stop a running server
- `cleanup_server()` - Clean up server and test data
- `cleanup_auth_server()` - Clean up auth server and test data

### lib/utils.sh
Utility functions for tests:
- `assert_success(message)` - Print success message
- `assert_failure(message)` - Print failure message and exit
- `test_header(message)` - Print test header
- `section_header(message)` - Print section header
- `section_success(message)` - Print success section
- `verify_aws_cli()` - Verify AWS CLI is installed
- `wait_for_server(addr, [max_attempts])` - Wait for server to be ready
- `build_server([binary_path])` - Build the s3d server
- `create_temp_dir()` - Create secure temporary directory
- `files_match(file1, file2)` - Compare two files
- `object_exists(endpoint, bucket, key, [sign])` - Check if object exists
- `bucket_exists(endpoint, bucket, [sign])` - Check if bucket exists

## Running Tests

### Run all tests
```bash
./test/e2e/run_all.sh
```

### Run a specific standalone test suite
```bash
# Run the example basic object tests suite
./test/e2e/suites/basic_object_tests.sh

# Run only auth tests
./test/e2e/auth_tests.sh
```

### Run specific test suites programmatically
```bash
# Source the libraries and run individual test files
source test/e2e/lib/config.sh
source test/e2e/lib/utils.sh
source test/e2e/lib/server.sh

# Initialize and start server
export E2E_TEST_DATA_DIR=$(create_temp_dir)
export E2E_SERVER_DATA_DIR=$(create_temp_dir)
trap cleanup_server EXIT
verify_aws_cli
start_server

# Run specific test suite
source test/e2e/bucket_tests.sh
test_create_bucket
test_list_buckets
```

### Using make
```bash
make test-e2e
```

## Environment Variables

You can customize test behavior with environment variables:

```bash
# Change server port
export E2E_SERVER_PORT=8080

# Change test bucket name
export E2E_TEST_BUCKET="my-test-bucket"

# Run tests
./test/e2e/run_all.sh
```

## Writing New Tests

### Using the new library structure

```bash
#!/bin/bash
# My new test suite

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/config.sh"
source "${SCRIPT_DIR}/lib/utils.sh"

test_my_feature() {
    test_header "Testing my feature"
    
    # Perform test operations
    aws --endpoint-url="${E2E_SERVER_ADDR}" --no-sign-request s3 mb "s3://${E2E_TEST_BUCKET}"
    
    # Verify result
    if bucket_exists "${E2E_SERVER_ADDR}" "${E2E_TEST_BUCKET}"; then
        assert_success "Feature works correctly"
    else
        assert_failure "Feature did not work"
    fi
}
```

### Best Practices

1. **Use the library functions** - Don't duplicate code
2. **Use configuration variables** - Use `E2E_*` variables instead of hardcoding
3. **Clean up resources** - Always clean up test data
4. **Use descriptive test names** - Make it clear what's being tested
5. **Add assertions** - Use `assert_success` and `assert_failure` for clear output
6. **Handle errors** - Tests should exit on failure

## Legacy Compatibility

The `common.sh` file provides backward compatibility with the old test structure. It maps old variable names to new ones:

- `SERVER_PORT` → `E2E_SERVER_PORT`
- `SERVER_ADDR` → `E2E_SERVER_ADDR`
- `TEST_BUCKET` → `E2E_TEST_BUCKET`
- `SERVER_PID` → `E2E_SERVER_PID`

Existing tests continue to work without modification.
