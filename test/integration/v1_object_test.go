package integration

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// TestV1ObjectOperations tests object-related S3 operations using AWS SDK v1
func TestV1ObjectOperations(t *testing.T) {
	tsV1 := setupTestServerV1(t)
	defer tsV1.cleanup()

	bucketName := "test-v1-object-operations"
	objectKey := "test-object.txt"
	objectContent := "Hello, S3! This is a test object."

	// Create bucket first
	_, err := tsV1.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test: Put object
	t.Run("PutObject", func(t *testing.T) {
		_, err := tsV1.client.PutObject(&s3.PutObjectInput{
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
		output, err := tsV1.client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Created object not found in list")
		}
	})

	// Test: List objects with ListObjects (v1)
	t.Run("ListObjects", func(t *testing.T) {
		output, err := tsV1.client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Created object not found in list")
		}
	})

	// Test: Get object
	t.Run("GetObject", func(t *testing.T) {
		output, err := tsV1.client.GetObject(&s3.GetObjectInput{
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
			t.Errorf("Expected content %q, got %q", objectContent, string(data))
		}
	})

	// Test: Copy object
	t.Run("CopyObject", func(t *testing.T) {
		copyKey := "test-object-copy.txt"
		_, err := tsV1.client.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(copyKey),
			CopySource: aws.String(bucketName + "/" + objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to copy object: %v", err)
		}

		// Verify copy exists
		output, err := tsV1.client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(copyKey),
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
			t.Errorf("Expected copied content %q, got %q", objectContent, string(data))
		}

		// Clean up copy
		_, err = tsV1.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(copyKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete copied object: %v", err)
		}
	})

	// Test: Delete object
	t.Run("DeleteObject", func(t *testing.T) {
		_, err := tsV1.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete object: %v", err)
		}

		// Verify deletion
		_, err = tsV1.client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err == nil {
			t.Fatal("Expected error when getting deleted object, got nil")
		}
	})

	// Clean up bucket
	_, err = tsV1.client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to delete bucket: %v", err)
	}
}
