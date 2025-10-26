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

s3d includes several types of tests:

- **Unit tests**: `make test`
- **Integration tests**: `make test-integration`
- **E2E tests**: `make test-e2e`
- **Compatibility tests**: `make test-compatibility`

### Compatibility Tests

S3 compatibility tests are run using [minio/mint](https://github.com/minio/mint), which tests various S3 clients and SDKs against the server to identify compatible and incompatible features.

Requirements:
- Docker or Podman

To run compatibility tests:
```bash
make test-compatibility
```

See [test/compatibility/README.md](test/compatibility/README.md) for more details.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/s3d/blob/master/LICENSE) for the full license text.
