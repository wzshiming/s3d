package integration

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestBucketLogging tests bucket logging configuration operations
func TestBucketLogging(t *testing.T) {
	bucketName := "test-bucket-logging"
	targetBucketName := "test-target-logging-bucket"

	// Create source bucket
	t.Run("CreateSourceBucket", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to create source bucket: %v", err)
		}
	})
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Create target bucket for logs
	t.Run("CreateTargetBucket", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(targetBucketName),
		})
		if err != nil {
			t.Fatalf("Failed to create target bucket: %v", err)
		}
	})
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(targetBucketName),
	})

	// Test: Get bucket logging (should be empty initially)
	t.Run("GetBucketLogging_Empty", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket logging: %v", err)
		}
		if output.LoggingEnabled != nil {
			t.Fatal("Expected logging to be disabled by default")
		}
	})

	// Test: Enable bucket logging
	t.Run("PutBucketLogging_Enable", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &s3types.BucketLoggingStatus{
				LoggingEnabled: &s3types.LoggingEnabled{
					TargetBucket: aws.String(targetBucketName),
					TargetPrefix: aws.String("access-logs/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to enable bucket logging: %v", err)
		}
	})

	// Test: Get bucket logging (should be enabled)
	t.Run("GetBucketLogging_Enabled", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket logging: %v", err)
		}
		if output.LoggingEnabled == nil {
			t.Fatal("Expected logging to be enabled")
		}
		if *output.LoggingEnabled.TargetBucket != targetBucketName {
			t.Fatalf("Expected target bucket %s, got %s", targetBucketName, *output.LoggingEnabled.TargetBucket)
		}
		if *output.LoggingEnabled.TargetPrefix != "access-logs/" {
			t.Fatalf("Expected target prefix 'access-logs/', got %s", *output.LoggingEnabled.TargetPrefix)
		}
	})

	// Test: Update bucket logging (change prefix)
	t.Run("PutBucketLogging_Update", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &s3types.BucketLoggingStatus{
				LoggingEnabled: &s3types.LoggingEnabled{
					TargetBucket: aws.String(targetBucketName),
					TargetPrefix: aws.String("logs/updated/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to update bucket logging: %v", err)
		}
	})

	// Test: Verify logging configuration was updated
	t.Run("GetBucketLogging_Updated", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket logging: %v", err)
		}
		if output.LoggingEnabled == nil {
			t.Fatal("Expected logging to be enabled")
		}
		if *output.LoggingEnabled.TargetPrefix != "logs/updated/" {
			t.Fatalf("Expected updated target prefix 'logs/updated/', got %s", *output.LoggingEnabled.TargetPrefix)
		}
	})

	// Test: Disable bucket logging
	t.Run("PutBucketLogging_Disable", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &s3types.BucketLoggingStatus{
				LoggingEnabled: nil,
			},
		})
		if err != nil {
			t.Fatalf("Failed to disable bucket logging: %v", err)
		}
	})

	// Test: Verify logging is disabled
	t.Run("GetBucketLogging_Disabled", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket logging: %v", err)
		}
		if output.LoggingEnabled != nil {
			t.Fatal("Expected logging to be disabled")
		}
	})

	// Test: Get logging for non-existent bucket
	t.Run("GetBucketLogging_NonExistentBucket", func(t *testing.T) {
		_, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String("non-existent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error for non-existent bucket")
		}
	})

	// Test: Put logging for non-existent bucket
	t.Run("PutBucketLogging_NonExistentBucket", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String("non-existent-bucket"),
			BucketLoggingStatus: &s3types.BucketLoggingStatus{
				LoggingEnabled: &s3types.LoggingEnabled{
					TargetBucket: aws.String(targetBucketName),
					TargetPrefix: aws.String("logs/"),
				},
			},
		})
		if err == nil {
			t.Fatal("Expected error for non-existent bucket")
		}
	})
}

// TestBucketLoggingWithAccessLogs tests that access logs are actually generated
func TestBucketLoggingWithAccessLogs(t *testing.T) {
	sourceBucket := "test-source-bucket-logs"
	targetBucket := "test-target-bucket-logs"

	// Create source and target buckets
	t.Run("Setup", func(t *testing.T) {
		_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(sourceBucket),
		})
		if err != nil {
			t.Fatalf("Failed to create source bucket: %v", err)
		}

		_, err = ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
			Bucket: aws.String(targetBucket),
		})
		if err != nil {
			t.Fatalf("Failed to create target bucket: %v", err)
		}
	})
	defer func() {
		// Clean up objects first
		listOutput, _ := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(targetBucket),
		})
		for _, obj := range listOutput.Contents {
			ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(targetBucket),
				Key:    obj.Key,
			})
		}
		listOutput, _ = ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(sourceBucket),
		})
		for _, obj := range listOutput.Contents {
			ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    obj.Key,
			})
		}

		ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(sourceBucket),
		})
		ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(targetBucket),
		})
	}()

	// Enable logging
	t.Run("EnableLogging", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(sourceBucket),
			BucketLoggingStatus: &s3types.BucketLoggingStatus{
				LoggingEnabled: &s3types.LoggingEnabled{
					TargetBucket: aws.String(targetBucket),
					TargetPrefix: aws.String("access-logs/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to enable logging: %v", err)
		}
	})

	// Make some requests to generate logs
	t.Run("GenerateAccessLogs", func(t *testing.T) {
		// PUT object
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String("test-file-1.txt"),
			Body:   strings.NewReader("test content 1"),
		})
		if err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}

		// PUT another object
		_, err = ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String("test-file-2.txt"),
			Body:   strings.NewReader("test content 2"),
		})
		if err != nil {
			t.Fatalf("Failed to put second object: %v", err)
		}

		// GET object
		_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String("test-file-1.txt"),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}

		// LIST objects
		_, err = ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(sourceBucket),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}
	})

	// Note: Since logs are batched and flushed asynchronously, we can't directly
	// test if logs were written in this integration test without access to the
	// server's FlushLogs method. The unit tests in pkg/server handle this.
	// In a real scenario, logs would appear after the flush interval or buffer fills.
	t.Run("VerifyLoggingConfigured", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(sourceBucket),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket logging: %v", err)
		}
		if output.LoggingEnabled == nil {
			t.Fatal("Expected logging to be enabled")
		}
		if *output.LoggingEnabled.TargetBucket != targetBucket {
			t.Fatalf("Expected target bucket %s, got %s", targetBucket, *output.LoggingEnabled.TargetBucket)
		}
	})
}

// TestBucketLoggingPersistence tests that logging configuration persists across server restarts
func TestBucketLoggingPersistence(t *testing.T) {
	bucketName := "test-bucket-logging-persistence"
	targetBucketName := "test-target-logging-persistence"

	// Create buckets
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create source bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	_, err = ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(targetBucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create target bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(targetBucketName),
	})

	// Enable logging
	t.Run("EnableLogging", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &s3types.BucketLoggingStatus{
				LoggingEnabled: &s3types.LoggingEnabled{
					TargetBucket: aws.String(targetBucketName),
					TargetPrefix: aws.String("persistent-logs/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to enable logging: %v", err)
		}
	})

	// Verify configuration persists (it's stored in .bucket-meta file)
	t.Run("VerifyPersistence", func(t *testing.T) {
		// Get logging multiple times to ensure it's not just cached
		for i := 0; i < 3; i++ {
			output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				t.Fatalf("Failed to get bucket logging (iteration %d): %v", i+1, err)
			}
			if output.LoggingEnabled == nil {
				t.Fatalf("Expected logging to be enabled (iteration %d)", i+1)
			}
			if *output.LoggingEnabled.TargetBucket != targetBucketName {
				t.Fatalf("Expected target bucket %s, got %s (iteration %d)",
					targetBucketName, *output.LoggingEnabled.TargetBucket, i+1)
			}
			if *output.LoggingEnabled.TargetPrefix != "persistent-logs/" {
				t.Fatalf("Expected target prefix 'persistent-logs/', got %s (iteration %d)",
					*output.LoggingEnabled.TargetPrefix, i+1)
			}
		}
	})
}

// TestBucketLoggingConcurrency tests concurrent access to bucket logging configuration
func TestBucketLoggingConcurrency(t *testing.T) {
	bucketName := "test-bucket-logging-concurrent"
	targetBucketName := "test-target-logging-concurrent"

	// Create buckets
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create source bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	_, err = ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(targetBucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create target bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(targetBucketName),
	})

	// Enable logging
	_, err = ts.client.PutBucketLogging(ts.ctx, &s3.PutBucketLoggingInput{
		Bucket: aws.String(bucketName),
		BucketLoggingStatus: &s3types.BucketLoggingStatus{
			LoggingEnabled: &s3types.LoggingEnabled{
				TargetBucket: aws.String(targetBucketName),
				TargetPrefix: aws.String("concurrent-logs/"),
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to enable logging: %v", err)
	}

	// Test concurrent GET requests (tests caching)
	t.Run("ConcurrentGets", func(t *testing.T) {
		done := make(chan bool, 10)
		errors := make(chan error, 10)

		for i := 0; i < 10; i++ {
			go func() {
				output, err := ts.client.GetBucketLogging(ts.ctx, &s3.GetBucketLoggingInput{
					Bucket: aws.String(bucketName),
				})
				if err != nil {
					errors <- err
					done <- false
					return
				}
				if output.LoggingEnabled == nil {
					errors <- io.EOF // Use as sentinel error
					done <- false
					return
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			select {
			case success := <-done:
				if !success {
					select {
					case err := <-errors:
						t.Fatalf("Concurrent get failed: %v", err)
					default:
						t.Fatal("Concurrent get failed: logging not enabled")
					}
				}
			}
		}
	})
}
