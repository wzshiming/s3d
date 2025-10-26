package server

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestMultipartUpload(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multipart-upload"
	objectKey := "test-multipart.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Test multipart upload
	t.Run("MultipartUpload", func(t *testing.T) {
		// Initiate multipart upload
		initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		uploadID := initOutput.UploadId

		// Upload parts
		part1Data := "Part 1 content - this is the first part of the multipart upload"
		part2Data := "Part 2 content - this is the second part of the multipart upload"

		part1Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       strings.NewReader(part1Data),
		})
		if err != nil {
			t.Fatalf("UploadPart 1 failed: %v", err)
		}

		part2Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       strings.NewReader(part2Data),
		})
		if err != nil {
			t.Fatalf("UploadPart 2 failed: %v", err)
		}

		// Complete multipart upload
		_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						PartNumber: aws.Int32(1),
						ETag:       part1Output.ETag,
					},
					{
						PartNumber: aws.Int32(2),
						ETag:       part2Output.ETag,
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}

		// Verify the object
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}

		expectedContent := part1Data + part2Data
		if string(data) != expectedContent {
			t.Fatalf("Expected content %q, got %q", expectedContent, string(data))
		}
	})
}

func TestListMultipartUploads(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-multipart-uploads"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multiple uploads
	uploads := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, key := range uploads {
		_, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}
	}

	// List multipart uploads
	output, err := ts.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	if len(output.Uploads) != 3 {
		t.Fatalf("Expected 3 uploads, got %d", len(output.Uploads))
	}
}

func TestListParts(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-parts"
	objectKey := "test-parts.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := initOutput.UploadId

	// Upload parts
	partData := []string{"part1 data", "part2 data", "part3 data"}
	for i, data := range partData {
		_, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(int32(i + 1)),
			Body:       strings.NewReader(data),
		})
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", i+1, err)
		}
	}

	// List parts
	output, err := ts.client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
	})
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}

	if len(output.Parts) != 3 {
		t.Fatalf("Expected 3 parts, got %d", len(output.Parts))
	}

	// Verify parts are in order
	for i, part := range output.Parts {
		if *part.PartNumber != int32(i+1) {
			t.Fatalf("Expected part number %d, got %d", i+1, *part.PartNumber)
		}
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-abort-multipart-upload"
	objectKey := "test-abort.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := initOutput.UploadId

	// Upload a part
	_, err = ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("test data"),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort multipart upload
	_, err = ts.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify upload is aborted - trying to upload another part should fail
	_, err = ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader("more data"),
	})
	if err == nil {
		t.Fatal("Expected error when uploading to aborted upload")
	}
}

func TestUploadPartInvalidUploadID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-part-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &bucketName,
		Key:        aws.String("test.txt"),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String("invalid-upload-id"),
		Body:       bytes.NewReader([]byte("data")),
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestCompleteMultipartUploadInvalidID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-complete-multipart-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucketName,
		Key:      aws.String("test.txt"),
		UploadId: aws.String("invalid-upload-id"),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{},
		},
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestAbortMultipartUploadInvalidID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-abort-multipart-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &bucketName,
		Key:      aws.String("test.txt"),
		UploadId: aws.String("invalid-upload-id"),
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestListPartsInvalidUploadID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-parts-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   &bucketName,
		Key:      aws.String("test.txt"),
		UploadId: aws.String("invalid-upload-id"),
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}
