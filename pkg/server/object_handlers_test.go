package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func TestRenameObject(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-rename-bucket"
	srcKey := "original.txt"
	dstKey := "renamed.txt"
	content := "Hello, this is original content!"

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

	// Rename object
	renameSource := fmt.Sprintf("%s/%s", bucketName, srcKey)
	_, err = ts.client.RenameObject(ctx, &s3.RenameObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String(dstKey),
		RenameSource: aws.String(renameSource),
	})
	if err != nil {
		t.Fatalf("RenameObject failed: %v", err)
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

	// Verify source object no longer exists
	_, err = ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(srcKey),
	})
	if err == nil {
		t.Fatalf("Expected error when getting renamed object from original location")
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

func TestListObjectsV2Pagination(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-objects-v2-pagination"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Create more objects than we'll request per page
	numObjects := 10
	testObjects := make([]string, numObjects)
	for i := 0; i < numObjects; i++ {
		testObjects[i] = fmt.Sprintf("object-%03d.txt", i)
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testObjects[i]),
			Body:   strings.NewReader(fmt.Sprintf("content-%d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", testObjects[i], err)
		}
	}
	defer func() {
		for _, key := range testObjects {
			ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
		}
	}()

	// Test pagination with MaxKeys
	t.Run("PaginationWithMaxKeys", func(t *testing.T) {
		maxKeys := int32(3)
		var allObjects []string

		// First page
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(maxKeys),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 first page failed: %v", err)
		}

		if len(output.Contents) > int(maxKeys) {
			t.Errorf("Expected at most %d objects in first page, got %d", maxKeys, len(output.Contents))
		}

		for _, obj := range output.Contents {
			allObjects = append(allObjects, *obj.Key)
		}

		// If there are more objects, IsTruncated should be true
		if len(testObjects) > int(maxKeys) && (output.IsTruncated == nil || !*output.IsTruncated) {
			t.Errorf("Expected IsTruncated=true when more objects exist than MaxKeys, got false")
		}

		// Continue fetching pages if truncated
		continuationToken := output.NextContinuationToken
		for output.IsTruncated != nil && *output.IsTruncated && continuationToken != nil {
			output, err = ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(bucketName),
				MaxKeys:           aws.Int32(maxKeys),
				ContinuationToken: continuationToken,
			})
			if err != nil {
				t.Fatalf("ListObjectsV2 continuation failed: %v", err)
			}

			for _, obj := range output.Contents {
				allObjects = append(allObjects, *obj.Key)
			}

			continuationToken = output.NextContinuationToken
		}

		// Verify we got all objects
		if len(allObjects) != numObjects {
			t.Errorf("Expected %d total objects across all pages, got %d", numObjects, len(allObjects))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, key := range allObjects {
			if seen[key] {
				t.Errorf("Duplicate object key found: %s", key)
			}
			seen[key] = true
		}
	})

	// Test pagination with StartAfter
	t.Run("PaginationWithStartAfter", func(t *testing.T) {
		startAfter := "object-004.txt"
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:     aws.String(bucketName),
			StartAfter: aws.String(startAfter),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 with StartAfter failed: %v", err)
		}

		// All returned objects should come after startAfter
		for _, obj := range output.Contents {
			if *obj.Key <= startAfter {
				t.Errorf("Object %s should come after StartAfter %s", *obj.Key, startAfter)
			}
		}

		// Should have 5 objects (005-009)
		expectedCount := 5
		if len(output.Contents) != expectedCount {
			t.Errorf("Expected %d objects after %s, got %d", expectedCount, startAfter, len(output.Contents))
		}
	})

	// Test small MaxKeys value
	t.Run("MaxKeysOne", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 with MaxKeys=1 failed: %v", err)
		}

		if len(output.Contents) != 1 {
			t.Errorf("Expected exactly 1 object with MaxKeys=1, got %d", len(output.Contents))
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			t.Errorf("Expected IsTruncated=true with MaxKeys=1 and %d total objects", numObjects)
		}

		if output.NextContinuationToken == nil {
			t.Errorf("Expected NextContinuationToken to be set when IsTruncated=true")
		}
	})

	// Test KeyCount field
	t.Run("KeyCountField", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(3),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if *output.KeyCount != int32(len(output.Contents)) {
			t.Errorf("KeyCount (%d) should equal number of Contents (%d)", *output.KeyCount, len(output.Contents))
		}
	})
}

func TestListObjectsV1Pagination(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-objects-v1-pagination"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Create more objects than we'll request per page
	numObjects := 10
	testObjects := make([]string, numObjects)
	for i := 0; i < numObjects; i++ {
		testObjects[i] = fmt.Sprintf("file-%03d.txt", i)
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testObjects[i]),
			Body:   strings.NewReader(fmt.Sprintf("content-%d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", testObjects[i], err)
		}
	}
	defer func() {
		for _, key := range testObjects {
			ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
		}
	}()

	// Test pagination with MaxKeys and Marker
	t.Run("PaginationWithMarker", func(t *testing.T) {
		maxKeys := int32(4)
		var allObjects []string

		// First page
		output, err := ts.client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(maxKeys),
		})
		if err != nil {
			t.Fatalf("ListObjects first page failed: %v", err)
		}

		if len(output.Contents) > int(maxKeys) {
			t.Errorf("Expected at most %d objects in first page, got %d", maxKeys, len(output.Contents))
		}

		for _, obj := range output.Contents {
			allObjects = append(allObjects, *obj.Key)
		}

		// If there are more objects, IsTruncated should be true
		if len(testObjects) > int(maxKeys) && (output.IsTruncated == nil || !*output.IsTruncated) {
			t.Errorf("Expected IsTruncated=true when more objects exist than MaxKeys, got false")
		}

		// Continue fetching pages if truncated
		marker := output.NextMarker
		for output.IsTruncated != nil && *output.IsTruncated {
			output, err = ts.client.ListObjects(ctx, &s3.ListObjectsInput{
				Bucket:  aws.String(bucketName),
				MaxKeys: aws.Int32(maxKeys),
				Marker:  marker,
			})
			if err != nil {
				t.Fatalf("ListObjects continuation failed: %v", err)
			}

			for _, obj := range output.Contents {
				allObjects = append(allObjects, *obj.Key)
			}

			marker = output.NextMarker
		}

		// Verify we got all objects
		if len(allObjects) != numObjects {
			t.Errorf("Expected %d total objects across all pages, got %d", numObjects, len(allObjects))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, key := range allObjects {
			if seen[key] {
				t.Errorf("Duplicate object key found: %s", key)
			}
			seen[key] = true
		}
	})

	// Test small MaxKeys value
	t.Run("MaxKeysOne", func(t *testing.T) {
		output, err := ts.client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("ListObjects with MaxKeys=1 failed: %v", err)
		}

		if len(output.Contents) != 1 {
			t.Errorf("Expected exactly 1 object with MaxKeys=1, got %d", len(output.Contents))
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			t.Errorf("Expected IsTruncated=true with MaxKeys=1 and %d total objects", numObjects)
		}

		if output.NextMarker == nil || *output.NextMarker == "" {
			t.Errorf("Expected NextMarker to be set when IsTruncated=true")
		}
	})
}

func TestDeleteObjects(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-delete-objects"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Create test objects
	testObjects := []string{"obj1.txt", "obj2.txt", "obj3.txt", "obj4.txt", "obj5.txt"}
	for _, key := range testObjects {
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader("test content"),
		})
		if err != nil {
			t.Fatalf("PutObject %s failed: %v", key, err)
		}
	}

	// Test DeleteObjects - delete multiple objects
	t.Run("DeleteMultipleObjects", func(t *testing.T) {
		objectsToDelete := []types.ObjectIdentifier{
			{Key: aws.String("obj1.txt")},
			{Key: aws.String("obj2.txt")},
			{Key: aws.String("obj3.txt")},
		}

		output, err := ts.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects failed: %v", err)
		}

		// Verify deleted objects are returned
		if len(output.Deleted) != 3 {
			t.Errorf("Expected 3 deleted objects, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
		}

		// Verify objects are actually deleted
		for _, obj := range objectsToDelete {
			_, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
			if err == nil {
				t.Errorf("Object %s should have been deleted", *obj.Key)
			}
		}
	})

	// Test DeleteObjects with quiet mode
	t.Run("DeleteObjectsQuietMode", func(t *testing.T) {
		objectsToDelete := []types.ObjectIdentifier{
			{Key: aws.String("obj4.txt")},
		}

		output, err := ts.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects with quiet mode failed: %v", err)
		}

		// In quiet mode, deleted objects should not be returned
		if len(output.Deleted) != 0 {
			t.Errorf("Expected 0 deleted objects in quiet mode, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
		}

		// Verify object is actually deleted
		_, err = ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("obj4.txt"),
		})
		if err == nil {
			t.Errorf("Object obj4.txt should have been deleted")
		}
	})

	// Test DeleteObjects with non-existent objects
	t.Run("DeleteNonexistentObjects", func(t *testing.T) {
		objectsToDelete := []types.ObjectIdentifier{
			{Key: aws.String("nonexistent1.txt")},
			{Key: aws.String("nonexistent2.txt")},
		}

		output, err := ts.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects with nonexistent objects failed: %v", err)
		}

		// S3 treats deleting nonexistent objects as success
		if len(output.Deleted) != 2 {
			t.Errorf("Expected 2 deleted objects, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
		}
	})

	// Test DeleteObjects with mixed existing and non-existing objects
	t.Run("DeleteMixedObjects", func(t *testing.T) {
		objectsToDelete := []types.ObjectIdentifier{
			{Key: aws.String("obj5.txt")},         // exists
			{Key: aws.String("nonexistent3.txt")}, // doesn't exist
		}

		output, err := ts.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects with mixed objects failed: %v", err)
		}

		// Both should be in deleted list
		if len(output.Deleted) != 2 {
			t.Errorf("Expected 2 deleted objects, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
		}
	})

	// Test DeleteObjects with invalid bucket
	t.Run("DeleteObjectsInvalidBucket", func(t *testing.T) {
		objectsToDelete := []types.ObjectIdentifier{
			{Key: aws.String("obj1.txt")},
		}

		_, err := ts.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String("nonexistent-bucket-12345"),
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent bucket, got nil")
		}

		if !strings.Contains(err.Error(), "NoSuchBucket") && !strings.Contains(err.Error(), "404") {
			t.Errorf("Expected NoSuchBucket error, got: %v", err)
		}
	})
}

func TestChecksumSHA256(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-checksum-sha256"
	objectKey := "checksum-test.txt"
	objectContent := "Hello, Checksum!"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	var expectedChecksum string

	// Test PutObject returns checksum
	t.Run("PutObjectReturnsChecksum", func(t *testing.T) {
		output, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		// Verify ETag is set
		if output.ETag == nil {
			t.Fatal("Expected ETag to be set")
		}

		// The SDK parses x-amz-checksum-sha256 header into ChecksumSHA256 field
		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set in PutObject response")
		}
		if *output.ChecksumSHA256 == "" {
			t.Fatal("ChecksumSHA256 should not be empty")
		}
		expectedChecksum = *output.ChecksumSHA256
		t.Logf("ChecksumSHA256: %s", expectedChecksum)
		t.Logf("ETag: %s", *output.ETag)
	})

	// Test GetObject returns checksum when checksum mode is enabled
	t.Run("GetObjectReturnsChecksum", func(t *testing.T) {
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(objectKey),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		// Read the body to consume it
		_, err = io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}

		// Verify ETag is set
		if output.ETag == nil {
			t.Fatal("Expected ETag to be set")
		}

		// When checksum mode is enabled, SDK should parse x-amz-checksum-sha256
		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set in GetObject response when ChecksumMode is enabled")
		}
		if *output.ChecksumSHA256 == "" {
			t.Fatal("ChecksumSHA256 should not be empty")
		}
		if *output.ChecksumSHA256 != expectedChecksum {
			t.Errorf("ChecksumSHA256 mismatch: got %s, want %s", *output.ChecksumSHA256, expectedChecksum)
		}
		t.Logf("ChecksumSHA256: %s", *output.ChecksumSHA256)
		t.Logf("ETag: %s", *output.ETag)
	})

	// Test HeadObject returns checksum when checksum mode is enabled
	t.Run("HeadObjectReturnsChecksum", func(t *testing.T) {
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(objectKey),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		// Verify ETag is set
		if output.ETag == nil {
			t.Fatal("Expected ETag to be set")
		}

		// When checksum mode is enabled, SDK should parse x-amz-checksum-sha256
		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set in HeadObject response when ChecksumMode is enabled")
		}
		if *output.ChecksumSHA256 == "" {
			t.Fatal("ChecksumSHA256 should not be empty")
		}
		if *output.ChecksumSHA256 != expectedChecksum {
			t.Errorf("ChecksumSHA256 mismatch: got %s, want %s", *output.ChecksumSHA256, expectedChecksum)
		}
		t.Logf("ChecksumSHA256: %s", *output.ChecksumSHA256)
		t.Logf("ETag: %s", *output.ETag)
	})

	// Cleanup
	ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
}
