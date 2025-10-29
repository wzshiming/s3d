package server

import (
	"context"
	"fmt"
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
		output, err := ts.client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("HeadBucket failed: %v", err)
		}
		
		// Verify that bucket region is returned
		if output.BucketRegion == nil {
			t.Error("Expected BucketRegion to be set")
		} else if *output.BucketRegion != "us-east-1" {
			t.Errorf("Expected BucketRegion to be 'us-east-1', got '%s'", *output.BucketRegion)
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

func TestListBucketsPagination(t *testing.T) {
	ctx := context.Background()

	// Create multiple buckets for pagination testing
	numBuckets := 10
	testBuckets := make([]string, numBuckets)
	for i := 0; i < numBuckets; i++ {
		testBuckets[i] = fmt.Sprintf("test-bucket-pagination-%03d", i)
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(testBuckets[i]),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket %s: %v", testBuckets[i], err)
		}
	}
	defer func() {
		for _, bucket := range testBuckets {
			ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
		}
	}()

	// Test pagination with MaxBuckets
	t.Run("PaginationWithMaxBuckets", func(t *testing.T) {
		maxBuckets := int32(3)
		var allBuckets []string

		// First page
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(maxBuckets),
			Prefix:     aws.String("test-bucket-pagination-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets first page failed: %v", err)
		}

		if len(output.Buckets) > int(maxBuckets) {
			t.Fatalf("Expected at most %d buckets in first page, got %d", maxBuckets, len(output.Buckets))
		}

		for _, bucket := range output.Buckets {
			allBuckets = append(allBuckets, *bucket.Name)
		}

		// If there's a continuation token, fetch next page
		if output.ContinuationToken != nil && *output.ContinuationToken != "" {
			output2, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
				MaxBuckets:        aws.Int32(maxBuckets),
				Prefix:            aws.String("test-bucket-pagination-"),
				ContinuationToken: output.ContinuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets second page failed: %v", err)
			}

			if len(output2.Buckets) > int(maxBuckets) {
				t.Fatalf("Expected at most %d buckets in second page, got %d", maxBuckets, len(output2.Buckets))
			}

			for _, bucket := range output2.Buckets {
				allBuckets = append(allBuckets, *bucket.Name)
			}
		}

		// Verify we got unique buckets
		uniqueBuckets := make(map[string]bool)
		for _, bucket := range allBuckets {
			if uniqueBuckets[bucket] {
				t.Fatalf("Duplicate bucket found: %s", bucket)
			}
			uniqueBuckets[bucket] = true
		}
	})

	// Test with MaxBuckets = 1
	t.Run("MaxBucketsOne", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(1),
			Prefix:     aws.String("test-bucket-pagination-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		if len(output.Buckets) != 1 {
			t.Fatalf("Expected exactly 1 bucket, got %d", len(output.Buckets))
		}

		if output.ContinuationToken == nil || *output.ContinuationToken == "" {
			t.Fatal("Expected continuation token when there are more buckets")
		}
	})

	// Test prefix filtering
	t.Run("PrefixFiltering", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			Prefix: aws.String("test-bucket-pagination-00"),
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		// Should only return buckets starting with "test-bucket-pagination-00"
		// This matches buckets: 000, 001, 002, 003, 004, 005, 006, 007, 008, 009 (exactly 10 buckets)
		expectedCount := 10
		if len(output.Buckets) != expectedCount {
			t.Fatalf("Expected %d buckets with prefix, got %d", expectedCount, len(output.Buckets))
		}

		prefix := "test-bucket-pagination-00"
		for _, bucket := range output.Buckets {
			if len(*bucket.Name) < len(prefix) || (*bucket.Name)[:len(prefix)] != prefix {
				t.Fatalf("Bucket %s does not match prefix filter %s", *bucket.Name, prefix)
			}
		}
	})

	// Test continuation token without prefix
	t.Run("ContinuationTokenOnly", func(t *testing.T) {
		// First, get some buckets
		output1, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(2),
			Prefix:     aws.String("test-bucket-pagination-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets first call failed: %v", err)
		}

		if output1.ContinuationToken == nil {
			t.Fatal("Expected continuation token from first call")
		}

		// Use continuation token to get next page
		output2, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			ContinuationToken: output1.ContinuationToken,
			Prefix:            aws.String("test-bucket-pagination-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets with continuation token failed: %v", err)
		}

		// Verify no buckets from first page are in second page
		firstPageBuckets := make(map[string]bool)
		for _, bucket := range output1.Buckets {
			firstPageBuckets[*bucket.Name] = true
		}

		for _, bucket := range output2.Buckets {
			if firstPageBuckets[*bucket.Name] {
				t.Fatalf("Bucket %s appeared in both pages", *bucket.Name)
			}
		}
	})
}

func TestListBucketsPrefix(t *testing.T) {
	ctx := context.Background()

	// Create buckets with different prefixes
	bucketA := "aaa-test-bucket"
	bucketB := "bbb-test-bucket"
	bucketC := "ccc-test-bucket"

	for _, bucket := range []string{bucketA, bucketB, bucketC} {
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket %s: %v", bucket, err)
		}
	}
	defer func() {
		for _, bucket := range []string{bucketA, bucketB, bucketC} {
			ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
		}
	}()

	// Test prefix filtering
	t.Run("FilterByPrefix", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			Prefix: aws.String("bbb-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		found := false
		for _, bucket := range output.Buckets {
			if *bucket.Name == bucketB {
				found = true
			}
			if *bucket.Name == bucketA || *bucket.Name == bucketC {
				t.Fatalf("Bucket %s should not be in results with prefix 'bbb-'", *bucket.Name)
			}
		}

		if !found {
			t.Fatal("Expected bucket bbb-test-bucket not found")
		}
	})
}
