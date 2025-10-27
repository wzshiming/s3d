# Quick Start Guide: S3 Compatibility Testing

This guide will help you run compatibility tests and understand the results.

## Running Your First Compatibility Test

1. **Make sure you have the prerequisites:**
   ```bash
   # Install Python dependencies
   pip3 install --user tox
   ```

2. **Run the tests:**
   ```bash
   cd /path/to/s3d
   make test-compatibility
   ```

3. **Wait for completion:**
   The tests will take several minutes to complete. You'll see:
   - Server starting
   - Ceph s3-tests being cloned and set up
   - Tests running
   - Results being generated

## Understanding the Results

### Output Files

After running, you'll find these files in `test/compatibility/`:

- **`compatibility_report.md`** - Main report with detailed analysis
- **`compatibility_results.txt`** - Raw pytest output
- **`s3d.log`** - Server logs (useful for debugging failures)

### Reading the Report

The compatibility report includes:

#### 1. Executive Summary
```
Overall Compatibility Score: 65.3%
```
This shows what percentage of tests passed.

#### 2. Feature Breakdown Table
Shows compatibility by feature category:
- ✅ Green (>90%) - Well supported
- ⚠️  Yellow (50-90%) - Partially supported  
- ❌ Red (<50%) - Not supported

#### 3. Supported Features
Lists all passing tests, organized by category:
```
### Bucket Operations
- ✓ test_bucket_create_delete
- ✓ test_bucket_head
...
```

#### 4. Unsupported Features
Lists failing tests - these indicate:
- Features not yet implemented
- Potential bugs
- Features intentionally not supported

## Common Test Results

### Expected Failures

Some tests are **expected to fail** because s3d doesn't implement these features:

- ❌ Versioning tests - s3d doesn't support bucket versioning
- ❌ ACL tests - s3d doesn't implement Access Control Lists
- ❌ Encryption tests - server-side encryption not implemented
- ❌ Lifecycle tests - lifecycle policies not supported
- ❌ Object locking tests - not implemented

These are documented limitations and not bugs.

### Unexpected Failures

If you see failures in these areas, they may indicate bugs:

- ❌ Basic bucket operations (create, delete, list)
- ❌ Basic object operations (put, get, delete)
- ❌ Multipart upload operations
- ❌ ListObjects v1 or v2

Check the detailed error messages in `compatibility_results.txt`.

## Troubleshooting

### Tests Won't Start

**Problem:** "Failed to start s3d server"

**Solution:**
1. Check if port is already in use
2. Review `s3d.log` for error messages
3. Try manually starting s3d: `./s3d -addr :8080 -data ./testdata`

### Dependency Errors

**Problem:** "tox: command not found"

**Solution:**
```bash
pip3 install --user tox
# Or use your package manager
sudo apt-get install tox  # Debian/Ubuntu
brew install tox          # macOS
```

### Clone Failures

**Problem:** "Failed to clone s3-tests repository"

**Solution:**
1. Check internet connection
2. Manually clone: `cd test/compatibility && git clone https://github.com/ceph/s3-tests.git`

## Running Specific Tests

To run only specific test categories:

```bash
cd test/compatibility/s3-tests
S3TEST_CONF=s3tests.conf tox -- s3tests/functional/test_s3.py -k bucket
```

Test category keywords:
- `bucket` - Bucket operations
- `object` - Object operations  
- `multipart` - Multipart uploads
- `list` - List operations
- `auth` - Authentication tests

## Comparing Results Over Time

Track improvements by comparing reports:

```bash
# Save current report
cp test/compatibility/compatibility_report.md reports/report-$(date +%Y%m%d).md

# After making changes, run tests again
make test-compatibility

# Compare
diff reports/report-20251026.md test/compatibility/compatibility_report.md
```

## Next Steps

After reviewing the results:

1. **Check known limitations** - Don't worry about expected failures
2. **Investigate unexpected failures** - These may be bugs worth fixing
3. **Track progress** - Re-run tests after implementing new features
4. **Share results** - Include compatibility reports in release notes

## Getting Help

- Review the [main README](README.md)
- Check the [example report](example_report.md) 
- Open an issue on GitHub with:
  - Your compatibility report
  - Server logs (`s3d.log`)
  - Steps to reproduce any unexpected failures

## Advanced: Running Against Remote Server

Test against a deployed s3d instance:

```bash
S3D_ENDPOINT=http://your-server.example.com:8080 ./run_compatibility.sh
```

This is useful for:
- Testing production deployments
- CI/CD pipelines
- Performance testing with different configurations
