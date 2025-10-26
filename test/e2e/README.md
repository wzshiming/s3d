# End-to-End (E2E) Tests

This directory contains end-to-end tests for the s3d server using the AWS CLI.

## Test Suites

The e2e tests are organized into the following test suites:

### 1. Bucket Tests (`bucket_tests.sh`)
Tests for basic bucket operations:
- List empty buckets
- Create bucket
- List buckets (with content)
- Delete bucket

### 2. Object Tests (`object_tests.sh`)
Tests for object operations:
- Upload files
- List objects
- Download files
- Upload multiple files
- List objects with prefix
- Copy objects
- Rename objects
- Delete objects
- Remove all objects

### 3. Advanced Tests (`advanced_tests.sh`)
Tests for advanced features:
- Multipart uploads (large files)
- Directory sync
- Upload part copy (multipart copy)
- Nested path handling

### 4. Authentication Tests (`auth_tests.sh`)
Tests for AWS Signature V4 authentication:
- Request without credentials (should fail)
- Request with wrong credentials (should fail)
- Request with valid credentials (should succeed)
- CRUD operations with authentication

## Running Tests

### Run All Tests
```bash
# From repository root
make test-e2e

# Or directly
./test/e2e/run_all.sh
```

### Run Specific Test Suite
Each test suite can be run independently:

```bash
# Run only bucket tests
./test/e2e/bucket_tests.sh

# Run only object tests
./test/e2e/object_tests.sh

# Run only advanced tests
./test/e2e/advanced_tests.sh

# Run only authentication tests
./test/e2e/auth_tests.sh
```

### Run Custom Test Combination
```bash
# Run specific suites using the main runner
./test/e2e/run_all.sh bucket object

# Run with authentication
./test/e2e/run_all.sh auth
```

## Prerequisites

- Go 1.x or later (to build the server)
- AWS CLI (`pip install awscli`)
- Bash shell

## Test Infrastructure

### Common Setup (`common.sh`)
Provides shared configuration and utilities:
- Server setup and teardown
- Environment configuration
- Color-coded output
- Temporary directory management

### Main Runner (`run_all.sh`)
Orchestrates test execution:
- Runs all test suites in order
- Provides summary of results
- Handles cleanup

## Adding New Tests

To add new tests:

1. Add test functions to appropriate test file (or create a new one)
2. Follow the naming convention: `test_<description>`
3. Use the shared color variables from `common.sh`:
   - `${GREEN}` - Success messages
   - `${RED}` - Error messages
   - `${YELLOW}` - Info messages
   - `${NC}` - No color (reset)
4. Make test files executable and self-contained
5. Source `common.sh` at the beginning
6. Add main execution block for standalone running

Example:
```bash
#!/bin/bash
set -e

# Source common utilities if not already sourced
if [ -z "$SERVER_ADDR" ]; then
    source "$(dirname "$0")/common.sh"
fi

test_my_feature() {
    echo -e "\n${YELLOW}Test: My feature${NC}"
    # Test implementation
    echo -e "${GREEN}✓ Test passed${NC}"
}

# Run tests if executed directly
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    setup
    test_my_feature
    echo -e "\n${GREEN}All tests passed!${NC}"
fi
```

## CI/CD Integration

These tests are automatically run in GitHub Actions:
- `.github/workflows/test.yml` - Main test workflow
- Job: `e2e-tests` - Runs all e2e tests

## Troubleshooting

### Server fails to start
- Check if port 9090 (or 9091 for auth tests) is already in use
- Ensure the s3d binary is built correctly

### AWS CLI not found
```bash
pip install awscli
```

### Tests fail with connection errors
- Ensure the server has enough time to start (30 second timeout)
- Check firewall settings

### Cleanup issues
- Tests automatically clean up temporary directories
- If cleanup fails, manually remove directories in `/tmp/tmp.*`
