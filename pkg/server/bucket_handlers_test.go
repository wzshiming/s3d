package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestBucketLogging(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket-logging"

	// Create bucket for testing
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Test GetBucketLogging - Empty by default
	t.Run("GetBucketLogging_Empty", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging failed: %v", err)
		}

		if output.LoggingEnabled != nil {
			t.Fatalf("Expected nil LoggingEnabled, got %+v", output.LoggingEnabled)
		}
	})

	// Create target bucket for logging
	targetBucketName := "test-bucket-logging-target"
	_, err = ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(targetBucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket for target failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(targetBucketName),
	})

	// Test PutBucketLogging
	t.Run("PutBucketLogging", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(targetBucketName),
					TargetPrefix: aws.String("logs/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("PutBucketLogging failed: %v", err)
		}
	})

	// Test GetBucketLogging - With configuration
	t.Run("GetBucketLogging_WithConfig", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging failed: %v", err)
		}

		if output.LoggingEnabled == nil {
			t.Fatal("Expected LoggingEnabled to be set")
		}

		if *output.LoggingEnabled.TargetBucket != targetBucketName {
			t.Fatalf("Expected TargetBucket %s, got %s", targetBucketName, *output.LoggingEnabled.TargetBucket)
		}

		if *output.LoggingEnabled.TargetPrefix != "logs/" {
			t.Fatalf("Expected TargetPrefix 'logs/', got %s", *output.LoggingEnabled.TargetPrefix)
		}
	})

	// Test PutBucketLogging - Clear configuration
	t.Run("PutBucketLogging_Clear", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket:              aws.String(bucketName),
			BucketLoggingStatus: &types.BucketLoggingStatus{},
		})
		if err != nil {
			t.Fatalf("PutBucketLogging (clear) failed: %v", err)
		}

		// Verify it's cleared
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging after clear failed: %v", err)
		}

		if output.LoggingEnabled != nil {
			t.Fatalf("Expected nil LoggingEnabled after clear, got %+v", output.LoggingEnabled)
		}
	})

	// Test GetBucketLogging - Nonexistent bucket
	t.Run("GetBucketLogging_NotFound", func(t *testing.T) {
		_, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String("nonexistent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent bucket, got nil")
		}
	})

	// Test PutBucketLogging - Nonexistent bucket
	t.Run("PutBucketLogging_NotFound", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String("nonexistent-bucket"),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(targetBucketName),
				},
			},
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent bucket, got nil")
		}
	})
}

