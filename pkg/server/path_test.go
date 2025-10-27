package server

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestNonASCIIObjectKeys tests that object keys with non-ASCII characters work correctly
func TestNonASCIIObjectKeys(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-non-ascii"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		content string
	}{
		{
			name:    "Chinese characters",
			key:     "测试文件.txt",
			content: "Chinese content",
		},
		{
			name:    "Japanese characters",
			key:     "テストファイル.txt",
			content: "Japanese content",
		},
		{
			name:    "Korean characters",
			key:     "테스트파일.txt",
			content: "Korean content",
		},
		{
			name:    "Emoji in key",
			key:     "file-😀-emoji.txt",
			content: "Emoji content",
		},
		{
			name:    "Mixed special characters",
			key:     "file (1) [2024].txt",
			content: "Special chars content",
		},
		{
			name:    "Spaces in key",
			key:     "file with spaces.txt",
			content: "Spaces content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Put object
			_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tt.key),
				Body:   strings.NewReader(tt.content),
			})
			if err != nil {
				t.Fatalf("PutObject failed for key %q: %v", tt.key, err)
			}

			// Get object
			output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tt.key),
			})
			if err != nil {
				t.Fatalf("GetObject failed for key %q: %v", tt.key, err)
			}
			defer output.Body.Close()

			data, err := io.ReadAll(output.Body)
			if err != nil {
				t.Fatalf("Failed to read object body: %v", err)
			}

			if string(data) != tt.content {
				t.Fatalf("Expected content %q, got %q", tt.content, string(data))
			}

			// Head object
			_, err = ts.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tt.key),
			})
			if err != nil {
				t.Fatalf("HeadObject failed for key %q: %v", tt.key, err)
			}

			// Delete object
			_, err = ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tt.key),
			})
			if err != nil {
				t.Fatalf("DeleteObject failed for key %q: %v", tt.key, err)
			}
		})
	}
}

// TestPathNormalization tests that path normalization works correctly
func TestPathNormalization(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-path-normalization"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put an object with a normal key
	normalKey := "folder/file.txt"
	content := "test content"

	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(normalKey),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify we can retrieve it
	output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(normalKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("Failed to read object body: %v", err)
	}

	if string(data) != content {
		t.Fatalf("Expected content %q, got %q", content, string(data))
	}
}
