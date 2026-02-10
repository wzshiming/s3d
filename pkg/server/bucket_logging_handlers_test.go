package server

import (
	"compress/gzip"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestBucketLogging(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket-logging"
	targetBucket := "test-bucket-logging-target"

	// Create buckets
	for _, b := range []string{bucketName, targetBucket} {
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(b),
		})
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}
	}
	defer func() {
		for _, b := range []string{bucketName, targetBucket} {
			ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(b),
			})
		}
	}()

	// Test GetBucketLogging - no logging configured
	t.Run("GetBucketLogging_Empty", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging failed: %v", err)
		}
		if output.LoggingEnabled != nil {
			t.Fatal("Expected LoggingEnabled to be nil for bucket without logging")
		}
	})

	// Test PutBucketLogging - enable logging
	t.Run("PutBucketLogging_Enable", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(targetBucket),
					TargetPrefix: aws.String("logs/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("PutBucketLogging failed: %v", err)
		}
	})

	// Test GetBucketLogging - verify logging is enabled
	t.Run("GetBucketLogging_Enabled", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging failed: %v", err)
		}
		if output.LoggingEnabled == nil {
			t.Fatal("Expected LoggingEnabled to be non-nil")
		}
		if aws.ToString(output.LoggingEnabled.TargetBucket) != targetBucket {
			t.Errorf("TargetBucket = %q, want %q", aws.ToString(output.LoggingEnabled.TargetBucket), targetBucket)
		}
		if aws.ToString(output.LoggingEnabled.TargetPrefix) != "logs/" {
			t.Errorf("TargetPrefix = %q, want %q", aws.ToString(output.LoggingEnabled.TargetPrefix), "logs/")
		}
	})

	// Test PutBucketLogging - disable logging
	t.Run("PutBucketLogging_Disable", func(t *testing.T) {
		_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket:              aws.String(bucketName),
			BucketLoggingStatus: &types.BucketLoggingStatus{},
		})
		if err != nil {
			t.Fatalf("PutBucketLogging failed: %v", err)
		}
	})

	// Test GetBucketLogging - verify logging is disabled
	t.Run("GetBucketLogging_Disabled", func(t *testing.T) {
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging failed: %v", err)
		}
		if output.LoggingEnabled != nil {
			t.Fatal("Expected LoggingEnabled to be nil after disabling logging")
		}
	})

	// Test PutBucketLogging - update logging config
	t.Run("PutBucketLogging_Update", func(t *testing.T) {
		// Enable logging
		_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(targetBucket),
					TargetPrefix: aws.String("first/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("PutBucketLogging (first) failed: %v", err)
		}

		// Update logging with different prefix
		_, err = ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
			Bucket: aws.String(bucketName),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(targetBucket),
					TargetPrefix: aws.String("second/"),
				},
			},
		})
		if err != nil {
			t.Fatalf("PutBucketLogging (second) failed: %v", err)
		}

		// Verify update
		output, err := ts.client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLogging failed: %v", err)
		}
		if output.LoggingEnabled == nil {
			t.Fatal("Expected LoggingEnabled to be non-nil")
		}
		if aws.ToString(output.LoggingEnabled.TargetPrefix) != "second/" {
			t.Errorf("TargetPrefix = %q, want %q", aws.ToString(output.LoggingEnabled.TargetPrefix), "second/")
		}
	})
}

func TestBucketLoggingWritesAccessLogs(t *testing.T) {
	ctx := context.Background()
	sourceBucket := "test-logging-source"
	targetBucket := "test-logging-target"

	// Create both buckets
	for _, b := range []string{sourceBucket, targetBucket} {
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(b),
		})
		if err != nil {
			t.Fatalf("CreateBucket %s failed: %v", b, err)
		}
	}
	defer func() {
		// Clean up objects in target bucket
		listOutput, _ := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(targetBucket),
		})
		if listOutput != nil {
			for _, obj := range listOutput.Contents {
				ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(targetBucket),
					Key:    obj.Key,
				})
			}
		}
		// Clean up objects in source bucket
		listOutput, _ = ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(sourceBucket),
		})
		if listOutput != nil {
			for _, obj := range listOutput.Contents {
				ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(sourceBucket),
					Key:    obj.Key,
				})
			}
		}
		for _, b := range []string{sourceBucket, targetBucket} {
			ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(b),
			})
		}
	}()

	// Enable logging on source bucket
	_, err := ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
		Bucket: aws.String(sourceBucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{
			LoggingEnabled: &types.LoggingEnabled{
				TargetBucket: aws.String(targetBucket),
				TargetPrefix: aws.String("access-logs/"),
			},
		},
	})
	if err != nil {
		t.Fatalf("PutBucketLogging failed: %v", err)
	}

	// Perform an operation on the source bucket (PutObject)
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String("test-object.txt"),
		Body:   strings.NewReader("hello world"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Perform another operation (GetObject)
	_, err = ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String("test-object.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	// Flush buffered access logs
	ts.handler.logFlusher.FlushAll()

	// Check that access logs were written to the target bucket
	listOutput, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(targetBucket),
		Prefix: aws.String("access-logs/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	if len(listOutput.Contents) < 1 {
		t.Fatalf("Expected at least 1 access log object, got %d", len(listOutput.Contents))
	}

	// Verify each log object starts with the expected prefix and ends with .gz
	for _, obj := range listOutput.Contents {
		key := aws.ToString(obj.Key)
		if !strings.HasPrefix(key, "access-logs/") {
			t.Errorf("Log key %q does not start with expected prefix 'access-logs/'", key)
		}
		if !strings.HasSuffix(key, ".gz") {
			t.Errorf("Log key %q does not end with '.gz' extension", key)
		}
	}

	// Read the content of the log object and verify it contains expected fields
	getOutput, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(targetBucket),
		Key:    listOutput.Contents[0].Key,
	})
	if err != nil {
		t.Fatalf("GetObject (log) failed: %v", err)
	}
	defer getOutput.Body.Close()

	// Decompress gzip content
	gzReader, err := gzip.NewReader(getOutput.Body)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	logContent, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to read log content: %v", err)
	}

	logStr := string(logContent)
	// Verify the log contains expected information
	if !strings.Contains(logStr, sourceBucket) {
		t.Errorf("Log entry does not contain source bucket name %q: %s", sourceBucket, logStr)
	}
	if !strings.Contains(logStr, "REST.") {
		t.Errorf("Log entry does not contain REST operation: %s", logStr)
	}
	// Verify multiple log lines are batched together
	lines := strings.Split(strings.TrimSpace(logStr), "\n")
	if len(lines) < 2 {
		t.Errorf("Expected at least 2 log lines in batched log object, got %d", len(lines))
	}

	// Test that disabling logging stops log writing
	_, err = ts.client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
		Bucket:              aws.String(sourceBucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{},
	})
	if err != nil {
		t.Fatalf("PutBucketLogging (disable) failed: %v", err)
	}

	// Count current log objects
	listBefore, _ := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(targetBucket),
		Prefix: aws.String("access-logs/"),
	})
	countBefore := len(listBefore.Contents)

	// Perform another operation
	_, err = ts.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String("test-object.txt"),
	})
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	// Flush to ensure any buffered logs would be written
	ts.handler.logFlusher.FlushAll()

	// Verify no new logs were written
	listAfter, _ := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(targetBucket),
		Prefix: aws.String("access-logs/"),
	})
	countAfter := len(listAfter.Contents)

	if countAfter != countBefore {
		t.Errorf("Expected no new logs after disabling, but got %d new log(s)", countAfter-countBefore)
	}
}
