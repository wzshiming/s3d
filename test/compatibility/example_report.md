# S3 Compatibility Test Results

**Generated:** 2025-10-26 12:30:00

**Test Suite:** Ceph s3-tests

**Duration:** 45.23s

## Executive Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total Tests** | 150 | 100% |
| ✅ Passed | 98 | 65.3% |
| ❌ Failed | 32 | 21.3% |
| ⊘ Skipped | 20 | 13.3% |
| ⚠️  Errors | 0 | 0.0% |

### 🎯 Overall Compatibility Score: **65.3%**

## Feature Compatibility Breakdown

| Feature Category | Passed | Failed | Skipped | Errors | Compatibility |
|-----------------|--------|--------|---------|--------|---------------|
| ✅ Bucket Operations | 15 | 2 | 1 | 0 | 83.3% |
| ✅ Object Operations | 28 | 3 | 2 | 0 | 84.8% |
| ✅ Multipart Upload | 12 | 1 | 0 | 0 | 92.3% |
| ✅ List Objects | 18 | 2 | 1 | 0 | 85.7% |
| ⚠️ Authentication | 10 | 5 | 2 | 0 | 58.8% |
| ⚠️ Headers & Metadata | 8 | 4 | 1 | 0 | 61.5% |
| ❌ Versioning | 0 | 5 | 5 | 0 | 0.0% |
| ❌ ACL | 2 | 8 | 6 | 0 | 12.5% |
| ❌ Encryption | 0 | 2 | 2 | 0 | 0.0% |
| ⚠️ Conditional Requests | 5 | 0 | 0 | 0 | 100.0% |

## ✅ Supported Features (Passing Tests)

The following 98 tests passed successfully:

### Bucket Operations

- ✓ `test_bucket_create_delete`
- ✓ `test_bucket_head`
- ✓ `test_bucket_list_distinct`
- ✓ `test_bucket_list_empty`
- ✓ `test_bucket_notexist`
- ✓ `test_bucket_recreate_notoverwriting`
- ✓ `test_list_buckets_anonymous`
- ✓ `test_list_buckets_invalid_auth`
- ✓ `test_bucket_create_exists`
- ✓ `test_bucket_list`
- ✓ `test_bucket_listv2`
- ✓ `test_bucket_create_naming_good_long`
- ✓ `test_bucket_create_naming_good_short`
- ✓ `test_bucket_delete_notexist`
- ✓ `test_bucket_head_extended`

### Object Operations

- ✓ `test_object_copy_to_itself`
- ✓ `test_object_copy_verify_contenttype`
- ✓ `test_object_write_read_update_read_delete`
- ✓ `test_get_object_ifmatch_good`
- ✓ `test_get_object_ifnonematch_good`
- ✓ `test_object_read_not_exist`
- ✓ `test_object_write`
- ✓ `test_object_head`
- ✓ `test_object_copy_same_bucket`
- ✓ `test_object_copy_diff_bucket`
- ✓ `test_object_copy_not_owned_bucket`
- ✓ `test_object_delete_key_bucket_gone`
- ✓ `test_object_raw_get`
- ✓ `test_object_raw_put`
- ✓ `test_object_raw_authenticated`
- ✓ `test_put_object`
- ✓ `test_get_object`
- ✓ `test_head_object`
- ✓ `test_delete_object`
- ✓ `test_object_copy_replace_metadata`
- ✓ `test_object_copy_bucket_not_found`
- ✓ `test_object_copy_key_not_found`
- ✓ `test_object_write_cache_control`
- ✓ `test_object_write_expires`
- ✓ `test_object_set_get_metadata_none_to_good`
- ✓ `test_object_set_get_metadata_none_to_empty`
- ✓ `test_object_set_get_unicode_metadata`
- ✓ `test_object_set_get_non_utf8_metadata`

### Multipart Upload

- ✓ `test_multipart_upload_empty`
- ✓ `test_multipart_upload_small`
- ✓ `test_multipart_upload_multiple_sizes`
- ✓ `test_multipart_upload_size_too_small`
- ✓ `test_abort_multipart_upload`
- ✓ `test_abort_multipart_upload_not_found`
- ✓ `test_list_multipart_upload`
- ✓ `test_multipart_upload_missing_part`
- ✓ `test_multipart_upload_incorrect_etag`
- ✓ `test_upload_part_copy_same_bucket`
- ✓ `test_upload_part_copy_cross_bucket`
- ✓ `test_multipart_copy_versioned`

### List Objects

- ✓ `test_bucket_list_empty`
- ✓ `test_bucket_list_distinct`
- ✓ `test_bucket_list_many`
- ✓ `test_bucket_listv2_many`
- ✓ `test_bucket_list_delimiter_basic`
- ✓ `test_bucket_list_delimiter_prefix`
- ✓ `test_bucket_list_delimiter_prefix_underscore`
- ✓ `test_bucket_list_delimiter_alt`
- ✓ `test_bucket_list_prefix_basic`
- ✓ `test_bucket_list_prefix_alt`
- ✓ `test_bucket_list_prefix_empty`
- ✓ `test_bucket_list_prefix_none`
- ✓ `test_bucket_list_prefix_not_exist`
- ✓ `test_bucket_list_maxkeys_one`
- ✓ `test_bucket_list_maxkeys_zero`
- ✓ `test_bucket_list_marker_none`
- ✓ `test_bucket_list_return_data`
- ✓ `test_bucket_listv2_fetchowner_notempty`

### Authentication

- ✓ `test_object_create_bad_authorization_empty`
- ✓ `test_object_create_bad_authorization_none`
- ✓ `test_object_create_bad_ua_empty_good`
- ✓ `test_object_create_bad_authorization_incorrect_aws4`
- ✓ `test_bucket_create_bad_authorization_invalid_aws4`
- ✓ `test_bucket_create_naming_bad_short_one`
- ✓ `test_bucket_create_naming_bad_short_two`
- ✓ `test_get_object_ifmatch_failed`
- ✓ `test_get_object_ifnonematch_failed`
- ✓ `test_bucket_list_objects_anonymous`

### Headers & Metadata

- ✓ `test_object_write_cache_control`
- ✓ `test_object_write_expires`
- ✓ `test_object_write_content_disposition`
- ✓ `test_object_write_content_encoding`
- ✓ `test_object_write_content_language`
- ✓ `test_object_set_get_metadata_none_to_good`
- ✓ `test_object_set_get_metadata_none_to_empty`
- ✓ `test_object_set_get_unicode_metadata`

### Conditional Requests

- ✓ `test_get_object_ifmatch_good`
- ✓ `test_get_object_ifnonematch_good`
- ✓ `test_get_object_ifmodifiedsince_good`
- ✓ `test_get_object_ifunmodifiedsince_good`
- ✓ `test_put_object_ifmatch`

## ❌ Unsupported/Failing Features

The following 32 tests failed:

### ACL

- ✗ `test_access_bucket_private_object_private`
- ✗ `test_access_bucket_private_object_publicread`
- ✗ `test_access_bucket_private_object_publicreadwrite`
- ✗ `test_access_bucket_publicread_object_private`
- ✗ `test_bucket_acl_default`
- ✗ `test_bucket_acl_canned_private`
- ✗ `test_object_acl_default`
- ✗ `test_object_acl_canned_publicread`

### Versioning

- ✗ `test_versioning_bucket_create_suspend`
- ✗ `test_versioning_bucket_create_suspend_delete`
- ✗ `test_versioning_obj_create_read_remove`
- ✗ `test_versioning_obj_create_read_remove_head`
- ✗ `test_versioning_obj_suspend_versions`

### Encryption

- ✗ `test_sse_kms_method_head`
- ✗ `test_sse_kms_present`

### Bucket Operations

- ✗ `test_bucket_create_naming_bad_punctuation`
- ✗ `test_bucket_create_naming_dns_underscore`

### Object Operations

- ✗ `test_object_copy_to_itself_with_metadata`
- ✗ `test_object_copy_zero_size`
- ✗ `test_object_write_to_nonexist_bucket`

### Authentication

- ✗ `test_object_create_bad_date_invalid_aws4`
- ✗ `test_object_create_bad_date_empty_aws4`
- ✗ `test_object_create_bad_date_unreadable_aws4`
- ✗ `test_object_create_bad_date_none_aws4`
- ✗ `test_object_create_bad_authorization_invalid_aws4`

### Headers & Metadata

- ✗ `test_object_create_bad_contentlength_empty`
- ✗ `test_object_create_bad_contentlength_negative`
- ✗ `test_object_create_bad_contentlength_none`
- ✗ `test_object_create_bad_expect_mismatch`

### Multipart Upload

- ✗ `test_multipart_upload_size_below_min`

### List Objects

- ✗ `test_bucket_list_maxkeys_invalid`
- ✗ `test_bucket_list_marker_bad`

## ⊘ Skipped Tests

The following 20 tests were skipped:

- `test_versioning_multi_object_delete`
- `test_versioning_copy_object`
- `test_versioning_obj_plain_null_version_removal`
- `test_lifecycle_expiration`
- `test_lifecycle_rules_conflicted`
- `test_bucket_policy_different_tenant`
- `test_bucket_policy_set_condition_operator_end_with_IfExists`
- `test_bucket_policy_another_bucket`
- `test_object_lock_put_obj_lock`
- `test_object_lock_get_obj_lock`
- `test_encryption_sse_c_present`
- `test_encryption_sse_c_other_key`
- `test_encryption_sse_c_invalid_md5`
- `test_encryption_sse_c_no_key`
- `test_encryption_sse_c_multipart_upload`
- `test_bucket_website_redirect_location`
- `test_bucket_website_redirect_location_trailing_slash_redirect`
- `test_bucket_website_private_index`
- `test_bucket_website_private_index_error`
- `test_logging_toggle`

## Known Limitations

Based on the s3d documentation, the following S3 features are **not yet implemented**:

- ❌ Bucket versioning
- ❌ Object locking
- ❌ Bucket policies
- ❌ ACLs (Access Control Lists)
- ❌ Server-side encryption
- ❌ Object tagging
- ❌ Lifecycle policies
- ❌ CORS (Cross-Origin Resource Sharing)
- ❌ Website hosting
- ❌ Logging
- ❌ Replication

## Currently Supported Features

The s3d server currently supports the following S3 features:

- ✅ Bucket operations (create, list, delete, head)
- ✅ Object operations (put, get, delete, head, copy)
- ✅ ListObjects v1 and v2 with prefix/delimiter
- ✅ Multipart uploads
- ✅ AWS Signature V4 authentication

## References

- [AWS S3 API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html)
- [Ceph s3-tests Repository](https://github.com/ceph/s3-tests)
- [s3d Repository](https://github.com/wzshiming/s3d)

---

*This report was automatically generated by the s3d compatibility test suite.*
