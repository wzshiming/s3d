package integration

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestBucketOperations tests bucket-related S3 operations
func TestBucketOperations(t *testing.T) {
	bucketName := "test-bucket-operations"

	// Test: List buckets (initially empty)
	t.Run("ListBuckets_Empty", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("Failed to list buckets: %v", err)
		}
		if len(output.Buckets) != 0 {
			t.Errorf("Expected 0 buckets, got %d", len(output.Buckets))
		}
	})

	// Test: Create bucket
	t.Run("CreateBucket", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}
	})

	// Test: Create bucket - Duplicate (should fail)
	t.Run("CreateBucket_Duplicate", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when creating duplicate bucket, got nil")
		}
	})

	// Test: List buckets
	t.Run("ListBuckets", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{})
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
		_, err := ts.client.HeadBucket(ts.ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to head existing bucket: %v", err)
		}
	})

	// Test: Delete bucket (after cleanup)
	t.Run("DeleteBucket", func(t *testing.T) {
		_, err := ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to delete bucket: %v", err)
		}
	})

	// Test: Delete bucket - Not Found
	t.Run("DeleteBucket_NotFound", func(t *testing.T) {
		_, err := ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when deleting non-existent bucket, got nil")
		}
	})

	// Test: Head bucket - Not Found
	t.Run("HeadBucket_NotFound", func(t *testing.T) {
		_, err := ts.client.HeadBucket(ts.ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("Expected error when heading non-existent bucket, got nil")
		}
	})
}

// TestListBucketsPagination tests ListBuckets pagination functionality
func TestListBucketsPagination(t *testing.T) {
	// Create multiple buckets for pagination testing
	numBuckets := 15
	testBuckets := make([]string, numBuckets)
	for i := 0; i < numBuckets; i++ {
		testBuckets[i] = fmt.Sprintf("int-test-bucket-%03d", i)
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(testBuckets[i]),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket %s: %v", testBuckets[i], err)
		}
	}
	defer func() {
		for _, bucket := range testBuckets {
			ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
		}
	}()

	// Test: Pagination with MaxBuckets
	t.Run("Pagination_MaxBuckets", func(t *testing.T) {
		maxBuckets := int32(5)
		var allBuckets []string

		// First page
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(maxBuckets),
			Prefix:     aws.String("int-test-bucket-"),
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

		if output.ContinuationToken == nil || *output.ContinuationToken == "" {
			t.Fatal("Expected continuation token when there are more buckets")
		}

		// Fetch remaining pages
		continuationToken := output.ContinuationToken
		for continuationToken != nil && *continuationToken != "" {
			output, err = ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
				MaxBuckets:        aws.Int32(maxBuckets),
				Prefix:            aws.String("int-test-bucket-"),
				ContinuationToken: continuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets pagination failed: %v", err)
			}

			for _, bucket := range output.Buckets {
				allBuckets = append(allBuckets, *bucket.Name)
			}

			continuationToken = output.ContinuationToken
		}

		// Verify we got all buckets
		if len(allBuckets) != numBuckets {
			t.Fatalf("Expected %d total buckets, got %d", numBuckets, len(allBuckets))
		}

		// Verify uniqueness
		uniqueBuckets := make(map[string]bool)
		for _, bucket := range allBuckets {
			if uniqueBuckets[bucket] {
				t.Fatalf("Duplicate bucket found: %s", bucket)
			}
			uniqueBuckets[bucket] = true
		}
	})

	// Test: Prefix filtering
	t.Run("Pagination_PrefixFilter", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			Prefix: aws.String("int-test-bucket-00"),
		})
		if err != nil {
			t.Fatalf("ListBuckets with prefix failed: %v", err)
		}

		// Should return buckets 000-009 (10 buckets)
		expectedCount := 10
		if len(output.Buckets) != expectedCount {
			t.Fatalf("Expected %d buckets with prefix 'int-test-bucket-00', got %d", expectedCount, len(output.Buckets))
		}

		// Verify all buckets match the prefix
		for _, bucket := range output.Buckets {
			if len(*bucket.Name) < 18 || (*bucket.Name)[:18] != "int-test-bucket-00" {
				t.Fatalf("Bucket %s does not match prefix filter", *bucket.Name)
			}
		}

		// Verify Prefix is echoed in response
		if output.Prefix == nil || *output.Prefix != "int-test-bucket-00" {
			t.Fatal("Expected Prefix field to be echoed in response")
		}
	})

	// Test: MaxBuckets = 1
	t.Run("Pagination_MaxBucketsOne", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(1),
			Prefix:     aws.String("int-test-bucket-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		if len(output.Buckets) != 1 {
			t.Fatalf("Expected exactly 1 bucket, got %d", len(output.Buckets))
		}

		if output.ContinuationToken == nil || *output.ContinuationToken == "" {
			t.Fatal("Expected continuation token when MaxBuckets=1 and more buckets exist")
		}
	})

	// Test: No pagination parameters (should return all buckets with default limit)
	t.Run("Pagination_DefaultBehavior", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			Prefix: aws.String("int-test-bucket-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		// All test buckets should be returned since we're under the default limit (10000)
		if len(output.Buckets) != numBuckets {
			t.Fatalf("Expected %d buckets without pagination, got %d", numBuckets, len(output.Buckets))
		}

		// Should not have a continuation token
		if output.ContinuationToken != nil && *output.ContinuationToken != "" {
			t.Fatal("Did not expect continuation token when all buckets fit in one page")
		}
	})

	// Test: Continuation token without prefix
	t.Run("Pagination_ContinuationTokenOnly", func(t *testing.T) {
		// First page
		output1, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(3),
			Prefix:     aws.String("int-test-bucket-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets first call failed: %v", err)
		}

		if output1.ContinuationToken == nil {
			t.Fatal("Expected continuation token from first call")
		}

		// Second page
		output2, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			ContinuationToken: output1.ContinuationToken,
			Prefix:            aws.String("int-test-bucket-"),
		})
		if err != nil {
			t.Fatalf("ListBuckets with continuation token failed: %v", err)
		}

		// Verify no overlap between pages
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
