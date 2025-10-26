# S3 Compatibility Tests

This directory contains compatibility tests for the s3d server based on the [Ceph s3-tests](https://github.com/ceph/s3-tests) test suite.

## Quick Start

**New to compatibility testing?** Check out the [Quick Start Guide](QUICKSTART.md) for step-by-step instructions.

## Overview

The compatibility tests verify s3d's implementation against the standard S3 API using the widely-adopted Ceph s3-tests suite. These tests help identify which S3 features are supported and which are not.

## Prerequisites

- Python 3.6 or later
- tox (install with `pip install tox`)
- A running s3d server (or the script will start one automatically)

## Running the Tests

### Quick Start

Run all compatibility tests:

```bash
./run_compatibility.sh
```

This script will:
1. Start an s3d server on a random port
2. Clone and set up the Ceph s3-tests repository
3. Run the test suite against the s3d server
4. Generate a compatibility report showing which features work and which don't

### Advanced Usage

Run specific test categories:

```bash
# Run only bucket tests
cd s3-tests
S3TEST_CONF=s3tests.conf tox -- s3tests/functional/test_s3.py -k bucket

# Run with verbose output
./run_compatibility.sh --verbose

# Use existing server
S3D_ENDPOINT=http://localhost:8080 ./run_compatibility.sh
```

## Test Results

Test results are saved to:
- `compatibility_results.txt` - Raw pytest output
- `compatibility_report.md` - Detailed markdown report with categorized results

See [example_report.md](example_report.md) for a sample of what the compatibility report looks like.

The report includes:
- ✓ PASS - Feature is compatible
- ✗ FAIL - Feature is not compatible or has issues
- ⊘ SKIP - Test was skipped

A summary shows the overall compatibility percentage and breaks down results by feature category.

## Configuration

The `s3tests.conf` file is automatically generated but can be customized:
- The script generates a basic configuration for anonymous/test access
- For advanced testing, modify the generated config in `s3-tests/s3tests.conf`

## Continuous Integration

The compatibility tests can be run in CI using the provided workflow:

```bash
make test-compatibility
```

## Interpreting Results

Not all test failures indicate bugs in s3d:
- Some tests are specific to Ceph RGW features not relevant to s3d
- Some tests check for optional S3 features not yet implemented (see Known Limitations below)
- Review the detailed output to understand the nature of each failure

### Known Limitations (Expected Failures)

The following S3 features are intentionally not implemented and will show as failures:
- Bucket versioning
- Object locking
- Bucket policies
- ACLs (Access Control Lists)
- Server-side encryption
- Object tagging
- Lifecycle policies

## Contributing

To add new compatibility tests or improve the framework:
1. Update the test filters in `run_compatibility.sh`
2. Add new test categories to the report generator in `generate_report.py`
3. Submit a pull request with your changes
