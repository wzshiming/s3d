package integration

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestObjectOperations tests object-related S3 operations
func TestObjectOperations(t *testing.T) {
	bucketName := "test-object-operations"
	objectKey := "test-object.txt"
	objectContent := "Hello, S3! This is a test object."

	// Create bucket first
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test: Put object
	t.Run("PutObject", func(t *testing.T) {
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}
	})

	// Test: List objects with ListObjectsV2
	t.Run("ListObjectsV2", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				if *obj.Size != int64(len(objectContent)) {
					t.Errorf("Object size mismatch: got %d, want %d", *obj.Size, len(objectContent))
				}
				break
			}
		}

		if !found {
			t.Fatal("Object not found in ListObjectsV2")
		}

		if *output.KeyCount != int32(len(output.Contents)) {
			t.Errorf("KeyCount mismatch: got %d, want %d", *output.KeyCount, len(output.Contents))
		}
	})

	// Test: List objects with ListObjects (v1)
	t.Run("ListObjects", func(t *testing.T) {
		output, err := ts.client.ListObjects(ts.ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects (v1): %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				if *obj.Size != int64(len(objectContent)) {
					t.Errorf("Object size mismatch: got %d, want %d", *obj.Size, len(objectContent))
				}
				break
			}
		}

		if !found {
			t.Fatal("Object not found in ListObjects (v1)")
		}
	})

	// Test: Get object
	t.Run("GetObject", func(t *testing.T) {
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Object content mismatch: got %q, want %q", string(data), objectContent)
		}
	})

	// Test: Copy object
	copiedKey := "copied-object.txt"
	t.Run("CopyObject", func(t *testing.T) {
		_, err := ts.client.CopyObject(ts.ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(copiedKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, objectKey)),
		})
		if err != nil {
			t.Fatalf("Failed to copy object: %v", err)
		}

		// Verify copied object
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(copiedKey),
		})
		if err != nil {
			t.Fatalf("Failed to get copied object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read copied object body: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Copied object content mismatch: got %q, want %q", string(data), objectContent)
		}
	})

	// Test: Delete object
	t.Run("DeleteObject", func(t *testing.T) {
		_, err := ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete object: %v", err)
		}

		// Verify object is deleted
		_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err == nil {
			t.Errorf("Expected error when getting deleted object")
		}
	})
}
