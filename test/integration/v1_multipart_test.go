package integration

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// TestV1MultipartUpload tests multipart upload operations using AWS SDK v1
func TestV1MultipartUpload(t *testing.T) {
	tsV1 := setupTestServerV1(t)
	defer tsV1.cleanup()

	bucketName := "test-v1-multipart"
	objectKey := "test-multipart-object.txt"

	// Create bucket first
	_, err := tsV1.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer tsV1.client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Test: Complete multipart upload
	t.Run("CompleteMultipartUpload", func(t *testing.T) {
		// Initiate multipart upload
		initResp, err := tsV1.client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to initiate multipart upload: %v", err)
		}

		uploadID := initResp.UploadId

		// Upload parts
		part1Data := []byte(strings.Repeat("a", 5*1024*1024)) // 5MB
		part1Resp, err := tsV1.client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			PartNumber: aws.Int64(1),
			UploadId:   uploadID,
			Body:       bytes.NewReader(part1Data),
		})
		if err != nil {
			t.Fatalf("Failed to upload part 1: %v", err)
		}

		part2Data := []byte(strings.Repeat("b", 5*1024*1024)) // 5MB
		part2Resp, err := tsV1.client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			PartNumber: aws.Int64(2),
			UploadId:   uploadID,
			Body:       bytes.NewReader(part2Data),
		})
		if err != nil {
			t.Fatalf("Failed to upload part 2: %v", err)
		}

		// Complete multipart upload
		_, err = tsV1.client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: []*s3.CompletedPart{
					{
						ETag:       part1Resp.ETag,
						PartNumber: aws.Int64(1),
					},
					{
						ETag:       part2Resp.ETag,
						PartNumber: aws.Int64(2),
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to complete multipart upload: %v", err)
		}

		// Verify object exists
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

		expectedLen := len(part1Data) + len(part2Data)
		if len(data) != expectedLen {
			t.Errorf("Expected object size %d, got %d", expectedLen, len(data))
		}

		// Clean up
		_, err = tsV1.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete object: %v", err)
		}
	})

	// Test: Abort multipart upload
	t.Run("AbortMultipartUpload", func(t *testing.T) {
		abortKey := "test-abort-multipart.txt"

		// Initiate multipart upload
		initResp, err := tsV1.client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(abortKey),
		})
		if err != nil {
			t.Fatalf("Failed to initiate multipart upload: %v", err)
		}

		uploadID := initResp.UploadId

		// Upload a part
		partData := []byte(strings.Repeat("c", 5*1024*1024)) // 5MB
		_, err = tsV1.client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(abortKey),
			PartNumber: aws.Int64(1),
			UploadId:   uploadID,
			Body:       bytes.NewReader(partData),
		})
		if err != nil {
			t.Fatalf("Failed to upload part: %v", err)
		}

		// Abort multipart upload
		_, err = tsV1.client.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(abortKey),
			UploadId: uploadID,
		})
		if err != nil {
			t.Fatalf("Failed to abort multipart upload: %v", err)
		}

		// Verify object does not exist
		_, err = tsV1.client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(abortKey),
		})
		if err == nil {
			t.Fatal("Expected error when getting aborted object, got nil")
		}
	})

	// Test: Upload part copy
	t.Run("UploadPartCopy", func(t *testing.T) {
		sourceKey := "source-for-copy.txt"
		targetKey := "target-copy.txt"
		sourceContent := strings.Repeat("d", 10*1024*1024) // 10MB

		// Create source object
		_, err := tsV1.client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(sourceKey),
			Body:   strings.NewReader(sourceContent),
		})
		if err != nil {
			t.Fatalf("Failed to create source object: %v", err)
		}
		defer tsV1.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(sourceKey),
		})

		// Initiate multipart upload for target
		initResp, err := tsV1.client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(targetKey),
		})
		if err != nil {
			t.Fatalf("Failed to initiate multipart upload: %v", err)
		}

		uploadID := initResp.UploadId

		// Copy part from source
		copyResp, err := tsV1.client.UploadPartCopy(&s3.UploadPartCopyInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(targetKey),
			PartNumber: aws.Int64(1),
			UploadId:   uploadID,
			CopySource: aws.String(bucketName + "/" + sourceKey),
		})
		if err != nil {
			t.Fatalf("Failed to upload part copy: %v", err)
		}

		// Complete multipart upload
		_, err = tsV1.client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(targetKey),
			UploadId: uploadID,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: []*s3.CompletedPart{
					{
						ETag:       copyResp.CopyPartResult.ETag,
						PartNumber: aws.Int64(1),
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to complete multipart upload: %v", err)
		}

		// Verify copied object
		output, err := tsV1.client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(targetKey),
		})
		if err != nil {
			t.Fatalf("Failed to get copied object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read copied object body: %v", err)
		}

		if string(data) != sourceContent {
			t.Errorf("Copied content does not match source")
		}

		// Clean up
		_, err = tsV1.client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(targetKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete copied object: %v", err)
		}
	})
}
