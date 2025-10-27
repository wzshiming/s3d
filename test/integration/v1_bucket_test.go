package integration

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// TestV1BucketOperations tests bucket-related S3 operations using AWS SDK v1
func TestV1BucketOperations(t *testing.T) {
	tsV1 := setupTestServerV1(t)
	defer tsV1.cleanup()

	bucketName := "test-v1-bucket-operations"

	// Test: List buckets (initially empty)
	t.Run("ListBuckets_Empty", func(t *testing.T) {
		output, err := tsV1.client.ListBuckets(&s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("Failed to list buckets: %v", err)
		}
		if len(output.Buckets) != 0 {
			t.Errorf("Expected 0 buckets, got %d", len(output.Buckets))
		}
	})

	// Test: Create bucket
	t.Run("CreateBucket", func(t *testing.T) {
		_, err := tsV1.client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}
	})

	// Test: Create bucket - Duplicate (should fail)
	t.Run("CreateBucket_Duplicate", func(t *testing.T) {
		_, err := tsV1.client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when creating duplicate bucket, got nil")
		}
	})

	// Test: List buckets
	t.Run("ListBuckets", func(t *testing.T) {
		output, err := tsV1.client.ListBuckets(&s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("Failed to list buckets: %v", err)
		}

		found := false
		for _, bucket := range output.Buckets {
			if *bucket.Name == bucketName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Created bucket not found in list")
		}
	})

	// Test: Head bucket - Exists
	t.Run("HeadBucket_Exists", func(t *testing.T) {
		_, err := tsV1.client.HeadBucket(&s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to head existing bucket: %v", err)
		}
	})

	// Test: Delete bucket (after cleanup)
	t.Run("DeleteBucket", func(t *testing.T) {
		_, err := tsV1.client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to delete bucket: %v", err)
		}
	})

	// Test: Delete bucket - Not Found
	t.Run("DeleteBucket_NotFound", func(t *testing.T) {
		_, err := tsV1.client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when deleting non-existent bucket, got nil")
		}
	})

	// Test: Head bucket - Not Found
	t.Run("HeadBucket_NotFound", func(t *testing.T) {
		_, err := tsV1.client.HeadBucket(&s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when heading non-existent bucket, got nil")
		}
	})
}
