package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestBucketOperations(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket-operations"

	// Test CreateBucket
	t.Run("CreateBucket", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}
	})

	// Test CreateBucket - Duplicate (should fail)
	t.Run("CreateBucket_Duplicate", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when creating duplicate bucket, got nil")
		}
	})

	// Test ListBuckets
	t.Run("ListBuckets", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		found := false
		for _, bucket := range output.Buckets {
			if *bucket.Name == bucketName {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("Bucket %s not found in list", bucketName)
		}
	})

	// Test HeadBucket - Exists
	t.Run("HeadBucket_Exists", func(t *testing.T) {
		_, err := ts.client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("HeadBucket failed: %v", err)
		}
	})

	// Test HeadBucket - Not Found
	t.Run("HeadBucket_NotFound", func(t *testing.T) {
		_, err := ts.client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String("nonexistent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent bucket, got nil")
		}
	})

	// Test DeleteBucket
	t.Run("DeleteBucket", func(t *testing.T) {
		_, err := ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}
	})

	// Test DeleteBucket - Not Found
	t.Run("DeleteBucket_NotFound", func(t *testing.T) {
		_, err := ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String("nonexistent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error when deleting nonexistent bucket, got nil")
		}
	})
}
