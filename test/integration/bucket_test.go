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

// TestBucketPagination tests pagination for ListBuckets operation
func TestBucketPagination(t *testing.T) {
	// Create multiple buckets for pagination testing
	numBuckets := 10
	bucketPrefix := "test-bucket-pagination"
	bucketNames := make([]string, numBuckets)

	for i := 0; i < numBuckets; i++ {
		bucketNames[i] = fmt.Sprintf("%s-%02d", bucketPrefix, i)
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketNames[i]),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket %s: %v", bucketNames[i], err)
		}
	}

	// Clean up at the end
	defer func() {
		for _, name := range bucketNames {
			ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(name),
			})
		}
	}()

	// Test: ListBuckets with MaxBuckets parameter
	t.Run("ListBuckets_WithMaxBuckets", func(t *testing.T) {
		maxBuckets := int32(3)
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			MaxBuckets: &maxBuckets,
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		if len(output.Buckets) > int(maxBuckets) {
			t.Errorf("Expected at most %d buckets, got %d", maxBuckets, len(output.Buckets))
		}

		// Should have continuation token if there are more results
		if len(output.Buckets) == int(maxBuckets) && output.ContinuationToken != nil {
			t.Logf("ContinuationToken present, more results available")
		}
	})

	// Test: ListBuckets pagination - iterate through all buckets
	t.Run("ListBuckets_PaginateAll", func(t *testing.T) {
		maxBuckets := int32(3)
		var allBuckets []string
		var continuationToken *string

		for {
			output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
				MaxBuckets:        &maxBuckets,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets failed: %v", err)
			}

			for _, bucket := range output.Buckets {
				allBuckets = append(allBuckets, *bucket.Name)
			}

			// Check for more results
			if output.ContinuationToken == nil {
				break
			}
			continuationToken = output.ContinuationToken

			// Safety check
			if len(allBuckets) > 100 {
				t.Fatal("Too many iterations, possible infinite loop")
			}
		}

		// Verify we got all our test buckets
		foundCount := 0
		for _, name := range allBuckets {
			for _, testBucket := range bucketNames {
				if name == testBucket {
					foundCount++
					break
				}
			}
		}

		if foundCount < numBuckets {
			t.Errorf("Expected to find at least %d test buckets, found %d", numBuckets, foundCount)
		}
	})

	// Test: ListBuckets pagination - no duplicates
	t.Run("ListBuckets_NoDuplicates", func(t *testing.T) {
		maxBuckets := int32(2)
		seenBuckets := make(map[string]bool)
		var continuationToken *string

		for {
			output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
				MaxBuckets:        &maxBuckets,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets failed: %v", err)
			}

			for _, bucket := range output.Buckets {
				name := *bucket.Name
				if seenBuckets[name] {
					t.Errorf("Bucket %s appeared in multiple pages", name)
				}
				seenBuckets[name] = true
			}

			// Check for more results
			if output.ContinuationToken == nil {
				break
			}
			continuationToken = output.ContinuationToken

			// Safety check
			if len(seenBuckets) > 100 {
				t.Fatal("Too many iterations, possible infinite loop")
			}
		}

		// Verify we got all our test buckets
		foundCount := 0
		for name := range seenBuckets {
			for _, testBucket := range bucketNames {
				if name == testBucket {
					foundCount++
					break
				}
			}
		}

		if foundCount < numBuckets {
			t.Errorf("Expected to find at least %d test buckets, found %d", numBuckets, foundCount)
		}
	})

	// Test: ListBuckets with invalid continuation token
	t.Run("ListBuckets_InvalidContinuationToken", func(t *testing.T) {
		// Using a continuation token beyond all bucket names
		invalidToken := "zzzzz-nonexistent-bucket"
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{
			ContinuationToken: &invalidToken,
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		// Should return empty results or only buckets after the token
		if output.ContinuationToken != nil {
			t.Error("Expected no continuation token for results beyond all buckets")
		}
	})

	// Test: ListBuckets without pagination parameters
	t.Run("ListBuckets_NoPagination", func(t *testing.T) {
		output, err := ts.client.ListBuckets(ts.ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}

		// Should return all buckets
		foundCount := 0
		for _, bucket := range output.Buckets {
			for _, testBucket := range bucketNames {
				if *bucket.Name == testBucket {
					foundCount++
					break
				}
			}
		}

		if foundCount < numBuckets {
			t.Errorf("Expected to find at least %d test buckets, found %d", numBuckets, foundCount)
		}
	})
}
