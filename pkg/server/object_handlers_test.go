package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestObjectOperations(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-object-operations"
	objectKey := "test-object.txt"
	objectContent := "Hello, S3!"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Test PutObject
	t.Run("PutObject", func(t *testing.T) {
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
	})

	// Test GetObject
	t.Run("GetObject", func(t *testing.T) {
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

		if string(data) != objectContent {
			t.Fatalf("Expected content %q, got %q", objectContent, string(data))
		}
	})

	// Test HeadObject
	t.Run("HeadObject", func(t *testing.T) {
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		if *output.ContentLength != int64(len(objectContent)) {
			t.Fatalf("Expected content length %d, got %d", len(objectContent), *output.ContentLength)
		}
	})

	// Test ListObjects
	t.Run("ListObjects", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				if *obj.Size != int64(len(objectContent)) {
					t.Fatalf("Expected size %d, got %d", len(objectContent), *obj.Size)
				}
				break
			}
		}

		if !found {
			t.Fatalf("Object %s not found in list", objectKey)
		}
	})

	// Test DeleteObject
	t.Run("DeleteObject", func(t *testing.T) {
		_, err := ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("DeleteObject failed: %v", err)
		}

		// Verify deletion
		_, err = ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err == nil {
			t.Fatalf("Expected error when getting deleted object")
		}
	})
}

func TestListObjectsWithPrefix(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-objects-with-prefix"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create objects with different prefixes
	objects := []string{
		"folder1/file1.txt",
		"folder1/file2.txt",
		"folder2/file1.txt",
		"root-file.txt",
	}

	for _, key := range objects {
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("test content"),
		})
		if err != nil {
			t.Fatalf("PutObject %s failed: %v", key, err)
		}
	}

	// Test listing with prefix
	t.Run("ListWithPrefix", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String("folder1/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if len(output.Contents) != 2 {
			t.Fatalf("Expected 2 objects, got %d", len(output.Contents))
		}
	})

	// Test listing with delimiter
	t.Run("ListWithDelimiter", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		// Should have 1 object (root-file.txt) and 2 common prefixes (folder1/, folder2/)
		if len(output.Contents) != 1 {
			t.Fatalf("Expected 1 object, got %d", len(output.Contents))
		}

		if len(output.CommonPrefixes) != 2 {
			t.Fatalf("Expected 2 common prefixes, got %d", len(output.CommonPrefixes))
		}
	})
}

func TestLargeObject(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-large-object"
	objectKey := "large-object.bin"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create a 10MB object
	size := 10 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Upload the object
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Download and verify
	output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer output.Body.Close()

	downloadedData, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("Failed to read object body: %v", err)
	}

	if len(downloadedData) != size {
		t.Fatalf("Expected size %d, got %d", size, len(downloadedData))
	}

	if !bytes.Equal(data, downloadedData) {
		t.Fatalf("Downloaded data does not match uploaded data")
	}
}

func TestCopyObject(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-copy-bucket"
	srcKey := "source.txt"
	dstKey := "destination.txt"
	content := "Hello, this is source content!"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(srcKey),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy object
	copySource := fmt.Sprintf("%s/%s", bucketName, srcKey)
	_, err = ts.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(dstKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify destination object exists
	output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(dstKey),
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

func TestHeadObjectNotFound(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-head-object-not-found"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    aws.String("nonexistent.txt"),
	})

	if err == nil {
		t.Fatal("Expected error for nonexistent object")
	}
}

func TestPutObjectInvalidBucket(t *testing.T) {
	ctx := context.Background()
	_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("nonexistent-bucket"),
		Key:    aws.String("test.txt"),
		Body:   bytes.NewReader([]byte("test")),
	})

	if err == nil {
		t.Fatal("Expected error for nonexistent bucket")
	}
}

func TestCopyObjectInvalidSource(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &bucketName,
		Key:        aws.String("dest.txt"),
		CopySource: aws.String("/nonexistent-bucket/source.txt"),
	})

	if err == nil {
		t.Fatal("Expected error for invalid copy source")
	}
}

func TestListObjectsV1(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-objects-v1"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Put some test objects
	testObjects := []string{"file1.txt", "file2.txt", "dir/file3.txt", "dir/file4.txt"}
	for _, key := range testObjects {
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("test content"),
		})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", key, err)
		}
	}

	// Test ListObjects v1 - List all
	t.Run("ListAll", func(t *testing.T) {
		output, err := ts.client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(output.Contents) != len(testObjects) {
			t.Errorf("Expected %d objects, got %d", len(testObjects), len(output.Contents))
		}
	})

	// Test ListObjects v1 - With prefix
	t.Run("WithPrefix", func(t *testing.T) {
		output, err := ts.client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String("dir/"),
		})
		if err != nil {
			t.Fatalf("ListObjects with prefix failed: %v", err)
		}

		if len(output.Contents) != 2 {
			t.Errorf("Expected 2 objects with prefix 'dir/', got %d", len(output.Contents))
		}
	})

	// Test ListObjects v1 - With delimiter
	t.Run("WithDelimiter", func(t *testing.T) {
		output, err := ts.client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjects with delimiter failed: %v", err)
		}

		// Should have 2 files and 1 common prefix "dir/"
		if len(output.Contents) != 2 {
			t.Errorf("Expected 2 top-level files, got %d", len(output.Contents))
		}
		if len(output.CommonPrefixes) != 1 {
			t.Errorf("Expected 1 common prefix, got %d", len(output.CommonPrefixes))
		}
	})

	// Clean up objects
	for _, key := range testObjects {
		ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
	}
}

func TestListObjectsV2(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-objects-v2"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Put some test objects
	testObjects := []string{"a.txt", "b.txt", "prefix/c.txt", "prefix/d.txt"}
	for _, key := range testObjects {
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("test content"),
		})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", key, err)
		}
	}

	// Test ListObjectsV2 - List all
	t.Run("ListAll", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if len(output.Contents) != len(testObjects) {
			t.Errorf("Expected %d objects, got %d", len(testObjects), len(output.Contents))
		}

		if *output.KeyCount != int32(len(testObjects)) {
			t.Errorf("Expected KeyCount %d, got %d", len(testObjects), *output.KeyCount)
		}
	})

	// Test ListObjectsV2 - With prefix
	t.Run("WithPrefix", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String("prefix/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 with prefix failed: %v", err)
		}

		if len(output.Contents) != 2 {
			t.Errorf("Expected 2 objects with prefix 'prefix/', got %d", len(output.Contents))
		}
	})

	// Test ListObjectsV2 - With delimiter
	t.Run("WithDelimiter", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 with delimiter failed: %v", err)
		}

		// Should have 2 files and 1 common prefix "prefix/"
		if len(output.Contents) != 2 {
			t.Errorf("Expected 2 top-level files, got %d", len(output.Contents))
		}
		if len(output.CommonPrefixes) != 1 {
			t.Errorf("Expected 1 common prefix, got %d", len(output.CommonPrefixes))
		}
	})

	// Test ListObjectsV2 - With max-keys
	t.Run("WithMaxKeys", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(2),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 with max-keys failed: %v", err)
		}

		if len(output.Contents) > 2 {
			t.Errorf("Expected at most 2 objects with max-keys=2, got %d", len(output.Contents))
		}
	})

	// Test ListObjectsV2 - Empty bucket
	t.Run("EmptyBucket", func(t *testing.T) {
		emptyBucket := "empty-test-bucket"
		_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(emptyBucket),
		})
		if err != nil {
			t.Fatalf("Failed to create empty bucket: %v", err)
		}
		defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(emptyBucket)})

		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(emptyBucket),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 on empty bucket failed: %v", err)
		}

		if len(output.Contents) != 0 {
			t.Errorf("Expected 0 objects in empty bucket, got %d", len(output.Contents))
		}
		if *output.KeyCount != 0 {
			t.Errorf("Expected KeyCount 0, got %d", *output.KeyCount)
		}
	})

	// Test ListObjectsV2 - Nonexistent bucket
	t.Run("NonexistentBucket", func(t *testing.T) {
		_, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String("nonexistent-bucket-12345"),
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent bucket, got nil")
		}

		var noSuchBucket *types.NoSuchBucket
		if !strings.Contains(err.Error(), "NoSuchBucket") && !strings.Contains(err.Error(), "404") {
			t.Errorf("Expected NoSuchBucket error, got: %v", err)
		}
		_ = noSuchBucket // Silence unused var
	})

	// Clean up objects
	for _, key := range testObjects {
		ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
	}
}

func TestRenameObject(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-rename-bucket"
	srcKey := "source.txt"
	targetKey := "renamed.txt"
	content := "Hello, this is source content!"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(srcKey),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Rename object using a custom HTTP request since AWS SDK doesn't have RenameObject
	targetURL := fmt.Sprintf("http://%s/%s/%s?targetKey=%s", ts.listener.Addr().String(), bucketName, srcKey, targetKey)
	req, err := http.NewRequest(http.MethodPost, targetURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("RenameObject request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("RenameObject failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Verify source object no longer exists
	_, err = ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(srcKey),
	})
	if err == nil {
		t.Fatalf("Expected error when getting source object after rename")
	}

	// Verify target object exists with correct content
	output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(targetKey),
	})
	if err != nil {
		t.Fatalf("GetObject on renamed object failed: %v", err)
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

func TestRenameObjectNotFound(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-rename-not-found"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Try to rename non-existent object
	targetURL := fmt.Sprintf("http://%s/%s/nonexistent.txt?targetKey=renamed.txt", ts.listener.Addr().String(), bucketName)
	req, err := http.NewRequest(http.MethodPost, targetURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("RenameObject request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 status for non-existent object, got %d", resp.StatusCode)
	}
}

func TestRenameObjectMissingTargetKey(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-rename-missing-target"
	srcKey := "source.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(srcKey),
		Body:   strings.NewReader("test content"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Try to rename without targetKey parameter
	targetURL := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, srcKey)
	req, err := http.NewRequest(http.MethodPost, targetURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("RenameObject request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected 400 status for missing targetKey, got %d", resp.StatusCode)
	}
}
