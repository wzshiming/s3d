# s3d

[![Tests](https://github.com/wzshiming/s3d/actions/workflows/test.yml/badge.svg)](https://github.com/wzshiming/s3d/actions/workflows/test.yml)
[![Build](https://github.com/wzshiming/s3d/actions/workflows/go-cross-build.yml/badge.svg)](https://github.com/wzshiming/s3d/actions/workflows/go-cross-build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/wzshiming/s3d.svg)](https://pkg.go.dev/github.com/wzshiming/s3d)

An S3-compatible server with local storage

## Features

- Bucket operations (create, list, delete, head)
- Object operations (put, get, delete, head, copy)
- ListObjects v1 and v2 with prefix/delimiter
- Multipart uploads
- AWS Signature V4 authentication

### Not yet implemented
- bucket versioning
- object locking
- bucket policies
- ACLs
- server-side encryption
- object tagging
- lifecycle policies
- etc.

## Testing

s3d includes comprehensive test coverage:

- **Unit tests**: `make test`
- **Integration tests**: `make test-integration`
- **E2E tests**: `make test-e2e`
- **Compatibility tests**: `make test-compatibility`

### S3 Compatibility Testing

The compatibility test suite uses the [Ceph s3-tests](https://github.com/ceph/s3-tests) to verify s3d's S3 API compatibility. These tests help identify which S3 features are supported and which are not.

To run compatibility tests:

```bash
make test-compatibility
```

This will:
1. Start an s3d server
2. Run the Ceph s3-tests suite
3. Generate a detailed compatibility report

For more information, see [test/compatibility/README.md](test/compatibility/README.md).

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/s3d/blob/master/LICENSE) for the full license text.
