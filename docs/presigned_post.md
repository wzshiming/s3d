# PresignedPost Implementation

This document describes the AWS S3 PresignedPost implementation added to s3d.

## What is PresignedPost?

PresignedPost is an AWS S3 feature that allows browser-based direct file uploads without exposing AWS credentials to the client. Instead of sending credentials, the server generates a signed policy document that the client includes in a multipart/form-data POST request.

## How it Works

1. **Server generates policy**: The server creates a JSON policy document with upload conditions
2. **Server signs policy**: The policy is Base64-encoded and signed using AWS Signature V4
3. **Client uploads file**: The client POSTs a multipart/form-data request
4. **Server validates**: The server validates the signature and checks all policy conditions
5. **Server stores file**: If valid, the file is stored at the specified location

## Usage Example (Ruby)

```ruby
require 'aws-sdk-s3'

s3 = Aws::S3::Resource.new(
  region: 'us-east-1',
  endpoint: 'http://localhost:9876',
  access_key_id: 'your-access-key',
  secret_access_key: 'your-secret-key',
  force_path_style: true
)

bucket = s3.bucket('my-bucket')

# Generate presigned POST
post = bucket.presigned_post(
  key: 'uploads/${filename}',
  success_action_status: '201',
  expires: 3600,
  content_length_range: 0..10485760
)
```

## Security

The implementation validates AWS Signature V4 signatures, checks policy expiration timestamps, and validates all policy conditions.
