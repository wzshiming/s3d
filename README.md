# s3d

[![Tests](https://github.com/wzshiming/s3d/actions/workflows/test.yml/badge.svg)](https://github.com/wzshiming/s3d/actions/workflows/test.yml)
[![Build](https://github.com/wzshiming/s3d/actions/workflows/build.yml/badge.svg)](https://github.com/wzshiming/s3d/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/wzshiming/s3d.svg)](https://pkg.go.dev/github.com/wzshiming/s3d)

An S3-compatible server with local storage

## Features

- Bucket operations (create, list, delete, head)
- Object operations (put, get, delete, head, copy, rename)
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

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/s3d/blob/master/LICENSE) for the full license text.
