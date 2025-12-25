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

func TestContentDisposition(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-content-disposition"
	objectKey := "test-file.txt"
	objectContent := "Hello, Content-Disposition!"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Test PutObject with Content-Disposition
	t.Run("PutObjectWithContentDisposition", func(t *testing.T) {
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:             aws.String(bucketName),
			Key:                aws.String(objectKey),
			Body:               strings.NewReader(objectContent),
			ContentDisposition: aws.String("attachment; filename=\"download.txt\""),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
	})

	// Test GetObject returns Content-Disposition
	t.Run("GetObjectReturnsContentDisposition", func(t *testing.T) {
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		if output.ContentDisposition == nil {
			t.Fatal("Expected ContentDisposition to be set")
		}

		if *output.ContentDisposition != "attachment; filename=\"download.txt\"" {
			t.Fatalf("Expected ContentDisposition 'attachment; filename=\"download.txt\"', got %q", *output.ContentDisposition)
		}

		// Verify content is correct
		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}
		if string(data) != objectContent {
			t.Fatalf("Expected content %q, got %q", objectContent, string(data))
		}
	})

	// Test HeadObject returns Content-Disposition
	t.Run("HeadObjectReturnsContentDisposition", func(t *testing.T) {
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		if output.ContentDisposition == nil {
			t.Fatal("Expected ContentDisposition to be set")
		}

		if *output.ContentDisposition != "attachment; filename=\"download.txt\"" {
			t.Fatalf("Expected ContentDisposition 'attachment; filename=\"download.txt\"', got %q", *output.ContentDisposition)
		}
	})

	// Test GetObject with response-content-disposition override
	t.Run("GetObjectWithResponseContentDispositionOverride", func(t *testing.T) {
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:                     aws.String(bucketName),
			Key:                        aws.String(objectKey),
			ResponseContentDisposition: aws.String("inline; filename=\"view.txt\""),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		if output.ContentDisposition == nil {
			t.Fatal("Expected ContentDisposition to be set")
		}

		// Should return the override value, not the stored one
		if *output.ContentDisposition != "inline; filename=\"view.txt\"" {
			t.Fatalf("Expected ContentDisposition 'inline; filename=\"view.txt\"', got %q", *output.ContentDisposition)
		}
	})

	// Test PutObject without Content-Disposition
	t.Run("PutObjectWithoutContentDisposition", func(t *testing.T) {
		objectKey2 := "test-file-no-cd.txt"
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey2),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		// GetObject should not return Content-Disposition
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey2),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		// Content-Disposition should be nil or empty
		if output.ContentDisposition != nil && *output.ContentDisposition != "" {
			t.Fatalf("Expected ContentDisposition to be empty, got %q", *output.ContentDisposition)
		}
	})

	// Test CopyObject preserves Content-Disposition
	t.Run("CopyObjectPreservesContentDisposition", func(t *testing.T) {
		destKey := "copied-file.txt"
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(destKey),
			CopySource: aws.String(bucketName + "/" + objectKey),
		})
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}

		// GetObject should return the same Content-Disposition
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		if output.ContentDisposition == nil {
			t.Fatal("Expected ContentDisposition to be set on copied object")
		}

		if *output.ContentDisposition != "attachment; filename=\"download.txt\"" {
			t.Fatalf("Expected ContentDisposition 'attachment; filename=\"download.txt\"', got %q", *output.ContentDisposition)
		}
	})
}

func TestMultipartUploadContentDisposition(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multipart-cd"
	objectKey := "multipart-file.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload with Content-Disposition
	t.Run("MultipartUploadWithContentDisposition", func(t *testing.T) {
		createOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:             aws.String(bucketName),
			Key:                aws.String(objectKey),
			ContentDisposition: aws.String("attachment; filename=\"multipart-download.txt\""),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		uploadID := createOutput.UploadId

		// Upload a part
		partData := []byte("Part 1 content for multipart upload test")
		uploadOutput, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(partData),
		})
		if err != nil {
			t.Fatalf("UploadPart failed: %v", err)
		}

		// Complete multipart upload
		_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       uploadOutput.ETag,
						PartNumber: aws.Int32(1),
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}

		// GetObject should return Content-Disposition
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		if output.ContentDisposition == nil {
			t.Fatal("Expected ContentDisposition to be set")
		}

		if *output.ContentDisposition != "attachment; filename=\"multipart-download.txt\"" {
			t.Fatalf("Expected ContentDisposition 'attachment; filename=\"multipart-download.txt\"', got %q", *output.ContentDisposition)
		}

		// Verify content is correct
		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}
		if string(data) != string(partData) {
			t.Fatalf("Expected content %q, got %q", string(partData), string(data))
		}
	})
}
