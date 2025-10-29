package integration

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestHeadBucketRegion tests that HeadBucket returns the region header
func TestHeadBucketRegion(t *testing.T) {
	bucketName := "test-bucket-region"

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Test: HeadBucket returns region
	t.Run("HeadBucket_ReturnsRegion", func(t *testing.T) {
		output, err := ts.client.HeadBucket(ts.ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to head bucket: %v", err)
		}

		if output.BucketRegion == nil {
			t.Error("Expected BucketRegion to be set")
		} else if *output.BucketRegion != "us-east-1" {
			t.Errorf("Expected BucketRegion to be 'us-east-1', got '%s'", *output.BucketRegion)
		}
	})

	// Test: HeadBucket for non-existent bucket
	t.Run("HeadBucket_NotFound", func(t *testing.T) {
		_, err := ts.client.HeadBucket(ts.ctx, &s3.HeadBucketInput{
			Bucket: aws.String("non-existent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error for non-existent bucket, got nil")
		}
	})
}
