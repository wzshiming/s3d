package integration

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestNonASCIIObjectKeys tests object operations with non-ASCII characters
func TestNonASCIIObjectKeys(t *testing.T) {
	bucketName := "test-nonascii-keys"

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	testCases := []struct {
		name    string
		key     string
		content string
	}{
		{
			name:    "Chinese characters",
			key:     "文件.txt",
			content: "Chinese file content",
		},
		{
			name:    "Cyrillic characters",
			key:     "файл.txt",
			content: "Cyrillic file content",
		},
		{
			name:    "Japanese characters",
			key:     "ファイル.txt",
			content: "Japanese file content",
		},
		{
			name:    "Greek characters",
			key:     "αρχείο.txt",
			content: "Greek file content",
		},
		{
			name:    "Emoji",
			key:     "file-😀.txt",
			content: "Emoji file content",
		},
		{
			name:    "Path with non-ASCII",
			key:     "文件夹/文件.txt",
			content: "Nested non-ASCII content",
		},
		{
			name:    "Mixed ASCII and non-ASCII",
			key:     "folder/文件夹/file-файл.txt",
			content: "Mixed content",
		},
	}

	// Test: Put objects with non-ASCII keys
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("PutObject_%s", tc.name), func(t *testing.T) {
			_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tc.key),
				Body:   strings.NewReader(tc.content),
			})
			if err != nil {
				t.Fatalf("Failed to put object with key %q: %v", tc.key, err)
			}
		})
	}

	// Test: List objects with non-ASCII keys
	t.Run("ListObjectsV2_NonASCII", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		if *output.KeyCount != int32(len(testCases)) {
			t.Errorf("Expected %d objects, got %d", len(testCases), *output.KeyCount)
		}

		// Verify all keys are present and correctly decoded
		foundKeys := make(map[string]bool)
		for _, obj := range output.Contents {
			foundKeys[*obj.Key] = true
		}

		for _, tc := range testCases {
			if !foundKeys[tc.key] {
				t.Errorf("Object with key %q not found in listing", tc.key)
			}
		}
	})

	// Test: Get objects with non-ASCII keys
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetObject_%s", tc.name), func(t *testing.T) {
			output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tc.key),
			})
			if err != nil {
				t.Fatalf("Failed to get object with key %q: %v", tc.key, err)
			}
			defer output.Body.Close()

			data, err := io.ReadAll(output.Body)
			if err != nil {
				t.Fatalf("Failed to read object body: %v", err)
			}

			if string(data) != tc.content {
				t.Errorf("Content mismatch for key %q: got %q, want %q", tc.key, string(data), tc.content)
			}
		})
	}

	// Test: Copy object with non-ASCII keys
	t.Run("CopyObject_NonASCII", func(t *testing.T) {
		srcKey := "源文件.txt"
		dstKey := "目标文件.txt"
		content := "Content to copy with non-ASCII keys"

		// Create source object
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(srcKey),
			Body:   strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("Failed to create source object: %v", err)
		}

		// Copy object
		_, err = ts.client.CopyObject(ts.ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(dstKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, srcKey)),
		})
		if err != nil {
			t.Fatalf("Failed to copy object: %v", err)
		}

		// Verify copied object
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dstKey),
		})
		if err != nil {
			t.Fatalf("Failed to get copied object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read copied object: %v", err)
		}

		if string(data) != content {
			t.Errorf("Copied object content mismatch: got %q, want %q", string(data), content)
		}
	})

	// Test: Rename object with non-ASCII keys
	t.Run("RenameObject_NonASCII", func(t *testing.T) {
		srcKey := "原名.txt"
		dstKey := "新名.txt"
		content := "Content to rename with non-ASCII keys"

		// Create source object
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(srcKey),
			Body:   strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("Failed to create source object: %v", err)
		}

		// Rename object
		_, err = ts.client.RenameObject(ts.ctx, &s3.RenameObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(dstKey),
			RenameSource: aws.String(fmt.Sprintf("%s/%s", bucketName, srcKey)),
		})
		if err != nil {
			t.Fatalf("Failed to rename object: %v", err)
		}

		// Verify renamed object exists
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dstKey),
		})
		if err != nil {
			t.Fatalf("Failed to get renamed object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read renamed object: %v", err)
		}

		if string(data) != content {
			t.Errorf("Renamed object content mismatch: got %q, want %q", string(data), content)
		}

		// Verify original object no longer exists
		_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(srcKey),
		})
		if err == nil {
			t.Errorf("Expected error when getting original object after rename")
		}
	})

	// Test: Delete objects with non-ASCII keys
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("DeleteObject_%s", tc.name), func(t *testing.T) {
			_, err := ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tc.key),
			})
			if err != nil {
				t.Fatalf("Failed to delete object with key %q: %v", tc.key, err)
			}

			// Verify object is deleted
			_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tc.key),
			})
			if err == nil {
				t.Errorf("Expected error when getting deleted object %q", tc.key)
			}
		})
	}
}
