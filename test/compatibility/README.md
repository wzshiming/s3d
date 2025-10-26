# S3D Compatibility Tests

This directory contains compatibility tests for s3d using the [minio/mint](https://github.com/minio/mint) testing framework.

## Overview

Minio Mint is a comprehensive S3 API compatibility testing framework that tests various S3 clients and SDKs against an S3-compatible server. These tests help identify which S3 features are compatible with s3d and which are not.

## Prerequisites

- **Docker** or **Podman**: Required to run the mint container
- **jq** (optional): For detailed test result analysis
  - Install on Ubuntu/Debian: `apt-get install jq`
  - Install on macOS: `brew install jq`

## Running Compatibility Tests

### Basic Usage

To run the compatibility tests with default settings:

```bash
cd test/compatibility
./run_mint.sh
```

This will:
1. Build the s3d server
2. Start the server on port 9000
3. Run minio/mint tests against it
4. Generate a compatibility report

### Configuration

You can customize the test run using environment variables:

```bash
# Change server port
SERVER_PORT=8080 ./run_mint.sh

# Use custom credentials
ACCESS_KEY=mykey SECRET_KEY=mysecret ./run_mint.sh

# Run full test suite (more comprehensive, takes longer)
MINT_MODE=full ./run_mint.sh

# Run core tests (faster, default)
MINT_MODE=core ./run_mint.sh
```

## Understanding the Results

The test script generates a compatibility report showing:

### Summary Statistics
- Total number of tests run
- Number of passed, failed, and not applicable tests
- Overall pass rate

### Results by SDK/Tool
Shows pass/fail counts for each SDK tested:
- aws-sdk-go
- aws-sdk-java
- aws-sdk-php
- minio-go
- minio-java
- minio-py
- awscli
- mc (minio client)
- s3cmd
- etc.

### Compatible Features
Lists S3 API functions that work correctly with s3d

### Incompatible Features
Lists S3 API functions that fail with s3d, indicating features that may need implementation or fixes

## Test Modes

Mint supports two test modes:

- **core** (default): Runs essential tests, faster execution
- **full**: Runs comprehensive test suite, takes longer

## Test Logs

Full test logs are stored in `test/compatibility/logs/log.json` in JSON format. Each test entry contains:
- `name`: SDK/tool name
- `function`: API function being tested
- `status`: PASS, FAIL, or NA
- `duration`: Test execution time
- `error`: Error details (if failed)

## Analyzing Results

To re-analyze results without re-running tests:

```bash
./parse_results.sh logs/log.json
```

## Known Limitations

Based on the s3d README, the following features are not yet implemented and will show as incompatible:
- Bucket versioning
- Object locking
- Bucket policies
- ACLs (Access Control Lists)
- Server-side encryption
- Object tagging
- Lifecycle policies

## Continuous Integration

To integrate compatibility tests in CI/CD:

```bash
# In your CI script
cd test/compatibility
./run_mint.sh
```

The script exits with status 0 on success (regardless of test failures, as some failures are expected for unimplemented features).

## Troubleshooting

### Container fails to connect to server
- Check firewall settings
- Verify the server port is not in use
- Check server logs in `/tmp/s3d.log`

### "Neither docker nor podman is installed"
Install Docker or Podman before running tests

### jq not found warning
Install jq for detailed result analysis, or ignore for basic summary
