package integration

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestMultipartUpload tests multipart upload functionality with edge cases
func TestMultipartUpload(t *testing.T) {
	bucketName := "multipart-bucket"
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test: Complete multipart upload
	t.Run("CompleteMultipartUpload", func(t *testing.T) {
		objectKey := "multipart-object.bin"

		// Initiate multipart upload
		createOutput, err := ts.client.CreateMultipartUpload(ts.ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to create multipart upload: %v", err)
		}
		uploadID := createOutput.UploadId

		// Upload parts
		part1Data := strings.Repeat("Part 1 ", 1000)
		part2Data := strings.Repeat("Part 2 ", 1000)

		part1Output, err := ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       strings.NewReader(part1Data),
		})
		if err != nil {
			t.Fatalf("Failed to upload part 1: %v", err)
		}

		part2Output, err := ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       strings.NewReader(part2Data),
		})
		if err != nil {
			t.Fatalf("Failed to upload part 2: %v", err)
		}

		// Complete multipart upload
		_, err = ts.client.CompleteMultipartUpload(ts.ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: part1Output.ETag},
					{PartNumber: aws.Int32(2), ETag: part2Output.ETag},
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to complete multipart upload: %v", err)
		}

		// Verify object exists and has correct size
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get multipart object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read multipart object: %v", err)
		}

		expectedContent := part1Data + part2Data
		if string(data) != expectedContent {
			t.Errorf("Multipart object content mismatch")
		}
	})

	// Test: Abort multipart upload
	t.Run("AbortMultipartUpload", func(t *testing.T) {
		objectKey := "aborted-multipart.bin"

		// Initiate multipart upload
		createOutput, err := ts.client.CreateMultipartUpload(ts.ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to create multipart upload: %v", err)
		}
		uploadID := createOutput.UploadId

		// Upload a part
		_, err = ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       strings.NewReader("test data"),
		})
		if err != nil {
			t.Fatalf("Failed to upload part: %v", err)
		}

		// Abort multipart upload
		_, err = ts.client.AbortMultipartUpload(ts.ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
		})
		if err != nil {
			t.Fatalf("Failed to abort multipart upload: %v", err)
		}

		// Verify object doesn't exist
		_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err == nil {
			t.Errorf("Expected error when getting aborted multipart object")
		}
	})
}
