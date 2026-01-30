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
- **s3fs-fuse compatibility** - supports directory objects (keys ending with `/`)

### Not yet implemented
- bucket versioning
- object locking
- bucket policies
- ACLs
- server-side encryption
- object tagging
- lifecycle policies
- etc.

## Compatibility

### s3fs-fuse

s3d is compatible with [s3fs-fuse](https://github.com/s3fs-fuse/s3fs-fuse), allowing you to mount S3 buckets as local filesystems. s3d properly handles:

- Directory objects (zero-byte objects with trailing slashes, e.g., `mydir/`)
- HEAD and GET requests on directory objects
- Listing objects with delimiters to show directory structure
- Nested directory hierarchies
- Distinction between files and directories with the same name (e.g., `file` vs `file/`)

This ensures that tools like s3fs-fuse can successfully mount and navigate directories stored in s3d buckets.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/s3d/blob/master/LICENSE) for the full license text.
