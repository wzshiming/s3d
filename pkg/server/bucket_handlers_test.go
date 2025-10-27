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

func TestListBucketsPagination(t *testing.T) {
	ctx := context.Background()
	
	// Create multiple buckets for pagination testing
	bucketPrefix := "test-list-buckets-pagination"
	numBuckets := 5
	bucketNames := make([]string, numBuckets)
	
	// Create buckets
	for i := 0; i < numBuckets; i++ {
		bucketNames[i] = fmt.Sprintf("%s-%02d", bucketPrefix, i)
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketNames[i]),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket %s: %v", bucketNames[i], err)
		}
	}
	
	// Clean up after test
	defer func() {
		for _, name := range bucketNames {
			ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(name),
			})
		}
	}()
	
	t.Run("PaginationWithMaxBuckets", func(t *testing.T) {
		// First page with MaxBuckets=2
		maxBuckets := int32(2)
		output1, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			MaxBuckets: &maxBuckets,
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}
		
		// Should return at most 2 buckets
		if len(output1.Buckets) > int(maxBuckets) {
			t.Fatalf("Expected at most %d buckets, got %d", maxBuckets, len(output1.Buckets))
		}
		
		// Check if there are more results
		if output1.ContinuationToken != nil {
			// Second page using continuation token
			output2, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
				MaxBuckets:        &maxBuckets,
				ContinuationToken: output1.ContinuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets with continuation token failed: %v", err)
			}
			
			// Verify we got different buckets
			if len(output2.Buckets) > 0 && len(output1.Buckets) > 0 {
				if *output1.Buckets[0].Name == *output2.Buckets[0].Name {
					t.Fatal("Second page returned same buckets as first page")
				}
			}
		}
	})
	
	t.Run("MaxBucketsOne", func(t *testing.T) {
		// Test with MaxBuckets=1 to ensure we can page through all buckets
		maxBuckets := int32(1)
		var allBuckets []string
		var continuationToken *string
		
		for {
			output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
				MaxBuckets:        &maxBuckets,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets failed: %v", err)
			}
			
			for _, bucket := range output.Buckets {
				allBuckets = append(allBuckets, *bucket.Name)
			}
			
			// Check pagination
			if output.ContinuationToken == nil {
				break
			}
			continuationToken = output.ContinuationToken
			
			// Safety check to prevent infinite loop
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
			t.Fatalf("Expected to find at least %d test buckets, found %d", numBuckets, foundCount)
		}
	})
	
	t.Run("NoPagination", func(t *testing.T) {
		// Test without pagination parameters - should return all buckets
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}
		
		// Verify we got all our test buckets
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
			t.Fatalf("Expected to find at least %d test buckets, found %d", numBuckets, foundCount)
		}
	})
	
	t.Run("NoDuplicatesAcrossPages", func(t *testing.T) {
		// Ensure no bucket appears in multiple pages
		maxBuckets := int32(2)
		var allBuckets []string
		seenBuckets := make(map[string]bool)
		var continuationToken *string
		
		for {
			output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
				MaxBuckets:        &maxBuckets,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				t.Fatalf("ListBuckets failed: %v", err)
			}
			
			for _, bucket := range output.Buckets {
				name := *bucket.Name
				if seenBuckets[name] {
					t.Fatalf("Bucket %s appeared in multiple pages", name)
				}
				seenBuckets[name] = true
				allBuckets = append(allBuckets, name)
			}
			
			// Check pagination
			if output.ContinuationToken == nil {
				break
			}
			continuationToken = output.ContinuationToken
			
			// Safety check to prevent infinite loop
			if len(allBuckets) > 100 {
				t.Fatal("Too many iterations, possible infinite loop")
			}
		}
		
		// Verify we got all our test buckets without duplicates
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
			t.Fatalf("Expected to find at least %d test buckets, found %d", numBuckets, foundCount)
		}
	})
	
	t.Run("EmptyResultsAfterLastBucket", func(t *testing.T) {
		// Test that using a continuation token beyond the last bucket returns empty results
		output, err := ts.client.ListBuckets(ctx, &s3.ListBucketsInput{
			ContinuationToken: aws.String("zzzzz-nonexistent-bucket"),
		})
		if err != nil {
			t.Fatalf("ListBuckets failed: %v", err)
		}
		
		// Should not be truncated and should have no continuation token
		if output.ContinuationToken != nil {
			t.Fatal("Expected no continuation token for empty results")
		}
	})
}
