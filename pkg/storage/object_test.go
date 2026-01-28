package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestObjectOperations(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	objectKey := "test-object.txt"
	objectContent := "Hello, World!"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put object
	objInfo, err := store.PutObject(bucketName, objectKey, bytes.NewReader([]byte(objectContent)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if objInfo.ETag == "" {
		t.Fatal("ETag should not be empty")
	}

	// Get object
	reader, info, err := store.GetObject(bucketName, objectKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}

	if string(data) != objectContent {
		t.Fatalf("Expected %q, got %q", objectContent, string(data))
	}

	if info.Key != objectKey {
		t.Fatalf("Expected key %q, got %q", objectKey, info.Key)
	}

	if info.Size != int64(len(objectContent)) {
		t.Fatalf("Expected size %d, got %d", len(objectContent), info.Size)
	}

	// List objects
	objects, _, err := store.ListObjects(bucketName, "", "", "", 0)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	if len(objects) != 1 {
		t.Fatalf("Expected 1 object, got %d", len(objects))
	}

	if objects[0].Key != objectKey {
		t.Fatalf("Expected key %q, got %q", objectKey, objects[0].Key)
	}

	// Delete object
	err = store.DeleteObject(bucketName, objectKey)
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify deletion
	_, _, err = store.GetObject(bucketName, objectKey)
	if err != ErrObjectNotFound {
		t.Fatal("Expected ErrObjectNotFound")
	}
}

func TestPathTraversalProtection(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()
	bucketName := "test-bucket-security"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Try to create object with path traversal
	testCases := []string{
		"../../../etc/passwd",
		"..\\..\\..\\windows\\system32",
		"./../../secret.txt",
		"/absolute/path/file.txt",
	}

	for _, key := range testCases {
		_, err := store.PutObject(bucketName, key, bytes.NewReader([]byte("test")), "text/plain")
		if err == nil {
			t.Fatalf("Expected error for path traversal attempt: %s", key)
		}
		if err != ErrInvalidObjectKey {
			t.Fatalf("Expected ErrInvalidObjectKey for %s, got %v", key, err)
		}
	}
}

func TestCopyObject(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()
	srcBucket := "test-bucket-copy-src"
	dstBucket := "test-bucket-copy-dst"
	srcKey := "source.txt"
	dstKey := "destination.txt"
	content := "Content to copy"

	// Create buckets
	err = store.CreateBucket(srcBucket)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	err = store.CreateBucket(dstBucket)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object
	_, err = store.PutObject(srcBucket, srcKey, bytes.NewReader([]byte(content)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy object
	objInfo, err := store.CopyObject(srcBucket, srcKey, dstBucket, dstKey)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	if objInfo.ETag == "" {
		t.Fatal("ETag should not be empty")
	}

	// Verify destination object
	reader, info, err := store.GetObject(dstBucket, dstKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}

	if string(data) != content {
		t.Fatalf("Expected %q, got %q", content, string(data))
	}

	if info.Size != int64(len(content)) {
		t.Fatalf("Expected size %d, got %d", len(content), info.Size)
	}
}

func TestGetNonexistentObject(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}

	_, _, err = store.GetObject("test-bucket", "nonexistent.txt")
	if err != ErrObjectNotFound {
		t.Fatalf("Expected ErrObjectNotFound, got %v", err)
	}
}

func TestObjectInvalidKeys(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}

	invalidKeys := []string{".", "..", "../file.txt"}
	for _, key := range invalidKeys {
		_, err := store.PutObject("test-bucket", key, bytes.NewReader([]byte("test")), "text/plain")
		if err != ErrInvalidObjectKey {
			t.Errorf("PutObject(%q) should return ErrInvalidObjectKey, got %v", key, err)
		}
	}
}

func TestCopyNonexistentObject(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CreateBucket("src"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateBucket("dst"); err != nil {
		t.Fatal(err)
	}

	_, err = store.CopyObject("src", "nonexistent.txt", "dst", "copy.txt")
	if err != ErrObjectNotFound {
		t.Fatalf("Expected ErrObjectNotFound, got %v", err)
	}
}

func TestListObjectsNonexistentBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, _, err = store.ListObjects("nonexistent", "", "", "", 0)
	if err != ErrBucketNotFound {
		t.Fatalf("Expected ErrBucketNotFound, got %v", err)
	}
}

func TestPutObjectNonexistentBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.PutObject("nonexistent", "key.txt", bytes.NewReader([]byte("test")), "text/plain")
	if err != ErrBucketNotFound {
		t.Fatalf("Expected ErrBucketNotFound, got %v", err)
	}
}

func TestRenameObject(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()
	bucketName := "test-bucket-rename"
	srcKey := "original.txt"
	dstKey := "renamed.txt"
	content := "Content to rename"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object
	_, err = store.PutObject(bucketName, srcKey, bytes.NewReader([]byte(content)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Rename object
	err = store.RenameObject(bucketName, srcKey, dstKey)
	if err != nil {
		t.Fatalf("RenameObject failed: %v", err)
	}

	// Verify destination object exists
	reader, info, err := store.GetObject(bucketName, dstKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}

	if string(data) != content {
		t.Fatalf("Expected %q, got %q", content, string(data))
	}

	if info.Size != int64(len(content)) {
		t.Fatalf("Expected size %d, got %d", len(content), info.Size)
	}

	// Verify source object no longer exists
	_, _, err = store.GetObject(bucketName, srcKey)
	if err != ErrObjectNotFound {
		t.Fatal("Expected ErrObjectNotFound for original object after rename")
	}
}

func TestRenameNonexistentObject(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}

	err = store.RenameObject("test-bucket", "nonexistent.txt", "renamed.txt")
	if err != ErrObjectNotFound {
		t.Fatalf("Expected ErrObjectNotFound, got %v", err)
	}
}

func TestRenameObjectNonexistentBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	err = store.RenameObject("nonexistent", "key.txt", "renamed.txt")
	if err != ErrBucketNotFound {
		t.Fatalf("Expected ErrBucketNotFound, got %v", err)
	}
}

func TestInlineDataForSmallFiles(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()
	bucketName := "test-bucket"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Test 1: Small file should be stored inline (<=4096 bytes)
	smallKey := "small.txt"
	smallContent := bytes.Repeat([]byte("x"), 100) // 100 bytes - well under threshold

	objInfo1, err := store.PutObject(bucketName, smallKey, bytes.NewReader(smallContent), "text/plain")
	if err != nil {
		t.Fatalf("PutObject for small file failed: %v", err)
	}

	// Verify data file doesn't exist for small files
	objectDir, _ := store.safePath(bucketName, smallKey)
	dataPath := objectDir + "/data"
	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Error("Small file should not have a separate data file")
	}

	// Verify meta file exists and contains data
	metaPath := objectDir + "/meta"
	if _, err := os.Stat(metaPath); err != nil {
		t.Errorf("Meta file should exist: %v", err)
	}

	// Read the file back
	reader1, info1, err := store.GetObject(bucketName, smallKey)
	if err != nil {
		t.Fatalf("GetObject for small file failed: %v", err)
	}
	defer reader1.Close()

	retrievedContent, err := io.ReadAll(reader1)
	if err != nil {
		t.Fatalf("Failed to read small file content: %v", err)
	}

	if !bytes.Equal(smallContent, retrievedContent) {
		t.Error("Retrieved content doesn't match original for small file")
	}

	if info1.Size != int64(len(smallContent)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(smallContent), info1.Size)
	}

	if info1.ETag != objInfo1.ETag {
		t.Error("ETag mismatch for small file")
	}

	// Test 2: Large file should use content-addressable storage (>4096 bytes)
	largeKey := "large.txt"
	largeContent := bytes.Repeat([]byte("y"), 5000) // 5000 bytes - over threshold

	objInfo2, err := store.PutObject(bucketName, largeKey, bytes.NewReader(largeContent), "text/plain")
	if err != nil {
		t.Fatalf("PutObject for large file failed: %v", err)
	}

	// Verify data file doesn't exist for large files (using content-addressable storage now)
	objectDir2, _ := store.safePath(bucketName, largeKey)
	dataPath2 := objectDir2 + "/data"
	if _, err := os.Stat(dataPath2); !os.IsNotExist(err) {
		t.Error("Large file should not have a separate data file in object directory (should be in .objects)")
	}

	// Read the large file back
	reader2, info2, err := store.GetObject(bucketName, largeKey)
	if err != nil {
		t.Fatalf("GetObject for large file failed: %v", err)
	}
	defer reader2.Close()

	retrievedContent2, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("Failed to read large file content: %v", err)
	}

	if !bytes.Equal(largeContent, retrievedContent2) {
		t.Error("Retrieved content doesn't match original for large file")
	}

	if info2.Size != int64(len(largeContent)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(largeContent), info2.Size)
	}

	if info2.ETag != objInfo2.ETag {
		t.Error("ETag mismatch for large file")
	}

	// Test 3: List objects should work correctly for both inline and separate data files
	objects, _, err := store.ListObjects(bucketName, "", "", "", 10)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	if len(objects) != 2 {
		t.Fatalf("Expected 2 objects, got %d", len(objects))
	}

	// Find our objects in the list
	var foundSmall, foundLarge bool
	for _, obj := range objects {
		if obj.Key == smallKey {
			foundSmall = true
			if obj.Size != int64(len(smallContent)) {
				t.Errorf("List: small file size mismatch: expected %d, got %d", len(smallContent), obj.Size)
			}
		}
		if obj.Key == largeKey {
			foundLarge = true
			if obj.Size != int64(len(largeContent)) {
				t.Errorf("List: large file size mismatch: expected %d, got %d", len(largeContent), obj.Size)
			}
		}
	}

	if !foundSmall || !foundLarge {
		t.Error("Not all objects found in list")
	}

	// Test 4: Copy small file should maintain inline storage
	copiedKey := "copied-small.txt"
	objInfoCopy, err := store.CopyObject(bucketName, smallKey, bucketName, copiedKey)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify copied file doesn't have separate data file
	copiedDir, _ := store.safePath(bucketName, copiedKey)
	copiedDataPath := copiedDir + "/data"
	if _, err := os.Stat(copiedDataPath); !os.IsNotExist(err) {
		t.Error("Copied small file should not have a separate data file")
	}

	// Verify content matches
	readerCopy, _, err := store.GetObject(bucketName, copiedKey)
	if err != nil {
		t.Fatalf("GetObject for copied file failed: %v", err)
	}
	defer readerCopy.Close()

	copiedContent, err := io.ReadAll(readerCopy)
	if err != nil {
		t.Fatalf("Failed to read copied file content: %v", err)
	}

	if !bytes.Equal(smallContent, copiedContent) {
		t.Error("Copied content doesn't match original")
	}

	if objInfoCopy.ETag != objInfo1.ETag {
		t.Error("ETag should be preserved when copying")
	}

	// Test 5: Delete should work for inline data files
	err = store.DeleteObject(bucketName, smallKey)
	if err != nil {
		t.Fatalf("DeleteObject for small file failed: %v", err)
	}

	// Verify object is deleted
	_, _, err = store.GetObject(bucketName, smallKey)
	if err != ErrObjectNotFound {
		t.Errorf("Expected ErrObjectNotFound after deletion, got %v", err)
	}
}

func TestInlineDataThreshold(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Test file exactly at threshold (4096 bytes)
	atThresholdKey := "at-threshold.txt"
	atThresholdContent := bytes.Repeat([]byte("z"), 4096)

	_, err = store.PutObject(bucketName, atThresholdKey, bytes.NewReader(atThresholdContent), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// File at threshold should be inline
	objectDir, _ := store.safePath(bucketName, atThresholdKey)
	dataPath := objectDir + "/data"
	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Error("File at threshold (4096 bytes) should be stored inline")
	}

	// Test file just above threshold (4097 bytes)
	aboveThresholdKey := "above-threshold.txt"
	aboveThresholdContent := bytes.Repeat([]byte("w"), 4097)

	_, err = store.PutObject(bucketName, aboveThresholdKey, bytes.NewReader(aboveThresholdContent), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// File above threshold should use content-addressable storage (no separate data file in object dir)
	objectDir2, _ := store.safePath(bucketName, aboveThresholdKey)
	dataPath2 := objectDir2 + "/data"
	if _, err := os.Stat(dataPath2); !os.IsNotExist(err) {
		t.Error("File above threshold (4097 bytes) should not have data file in object directory (should be in .objects)")
	}
}

// TestPutObjectDuplicateCompatibility tests putting the same object multiple times
func TestPutObjectDuplicateCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Put same object twice (same content)
	t.Run("SameContentTwice", func(t *testing.T) {
		objectKey := "duplicate-same.txt"
		content := bytes.Repeat([]byte("test"), 100)

		// First put
		objInfo1, err := store.PutObject(bucketName, objectKey, bytes.NewReader(content), "text/plain")
		if err != nil {
			t.Fatalf("First PutObject failed: %v", err)
		}

		// Second put with same content - should be compatible
		objInfo2, err := store.PutObject(bucketName, objectKey, bytes.NewReader(content), "text/plain")
		if err != nil {
			t.Fatalf("Second PutObject with same content failed: %v", err)
		}

		// ETags should be the same
		if objInfo1.ETag != objInfo2.ETag {
			t.Errorf("Expected same ETag for same content, got %s and %s", objInfo1.ETag, objInfo2.ETag)
		}

		// Verify object still exists and has correct content
		reader, info, err := store.GetObject(bucketName, objectKey)
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Error("Content doesn't match original")
		}
		if info.ETag != objInfo1.ETag {
			t.Error("ETag doesn't match")
		}
	})

	// Test 2: Put object with different content (overwrite)
	t.Run("DifferentContentOverwrite", func(t *testing.T) {
		objectKey := "duplicate-different.txt"
		content1 := []byte("first content")
		content2 := []byte("second content different")

		// First put
		objInfo1, err := store.PutObject(bucketName, objectKey, bytes.NewReader(content1), "text/plain")
		if err != nil {
			t.Fatalf("First PutObject failed: %v", err)
		}

		// Second put with different content - should overwrite
		objInfo2, err := store.PutObject(bucketName, objectKey, bytes.NewReader(content2), "text/plain")
		if err != nil {
			t.Fatalf("Second PutObject with different content failed: %v", err)
		}

		// ETags should be different
		if objInfo1.ETag == objInfo2.ETag {
			t.Errorf("Expected different ETags for different content")
		}

		// Verify object has new content
		reader, info, err := store.GetObject(bucketName, objectKey)
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content2) {
			t.Error("Content should be updated to second version")
		}
		if info.ETag != objInfo2.ETag {
			t.Error("ETag should be from second version")
		}
	})
}

// TestCopyObjectDuplicateCompatibility tests copying to an existing destination
func TestCopyObjectDuplicateCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Copy to existing destination with same content
	t.Run("SameContentAtDestination", func(t *testing.T) {
		srcKey := "source1.txt"
		dstKey := "dest1.txt"
		content := []byte("shared content")

		// Create source
		_, err := store.PutObject(bucketName, srcKey, bytes.NewReader(content), "text/plain")
		if err != nil {
			t.Fatalf("PutObject source failed: %v", err)
		}

		// Create destination with same content
		_, err = store.PutObject(bucketName, dstKey, bytes.NewReader(content), "text/plain")
		if err != nil {
			t.Fatalf("PutObject destination failed: %v", err)
		}

		// Copy - should detect same content and be compatible
		objInfo, err := store.CopyObject(bucketName, srcKey, bucketName, dstKey)
		if err != nil {
			t.Fatalf("CopyObject to existing destination with same content failed: %v", err)
		}

		if objInfo.ETag == "" {
			t.Error("ETag should not be empty")
		}

		// Verify destination still exists with correct content
		reader, _, err := store.GetObject(bucketName, dstKey)
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Error("Destination content should remain unchanged")
		}
	})

	// Test 2: Copy to existing destination with different content (overwrite)
	t.Run("DifferentContentAtDestination", func(t *testing.T) {
		srcKey := "source2.txt"
		dstKey := "dest2.txt"
		srcContent := []byte("source content")
		dstContent := []byte("destination content different")

		// Create source
		_, err := store.PutObject(bucketName, srcKey, bytes.NewReader(srcContent), "text/plain")
		if err != nil {
			t.Fatalf("PutObject source failed: %v", err)
		}

		// Create destination with different content
		_, err = store.PutObject(bucketName, dstKey, bytes.NewReader(dstContent), "text/plain")
		if err != nil {
			t.Fatalf("PutObject destination failed: %v", err)
		}

		// Copy - should overwrite destination
		objInfo, err := store.CopyObject(bucketName, srcKey, bucketName, dstKey)
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}

		if objInfo.ETag == "" {
			t.Error("ETag should not be empty")
		}

		// Verify destination has source content now
		reader, _, err := store.GetObject(bucketName, dstKey)
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, srcContent) {
			t.Error("Destination should have source content after copy")
		}
	})
}

// TestRenameObjectDuplicateCompatibility tests renaming to an existing destination
func TestRenameObjectDuplicateCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Rename to existing destination with same content (compatible)
	t.Run("SameContentAtDestination", func(t *testing.T) {
		srcKey := "rename-src1.txt"
		dstKey := "rename-dst1.txt"
		content := []byte("same content")

		// Create source
		_, err := store.PutObject(bucketName, srcKey, bytes.NewReader(content), "text/plain")
		if err != nil {
			t.Fatalf("PutObject source failed: %v", err)
		}

		// Create destination with same content
		_, err = store.PutObject(bucketName, dstKey, bytes.NewReader(content), "text/plain")
		if err != nil {
			t.Fatalf("PutObject destination failed: %v", err)
		}

		// Rename - should succeed because content is the same (compatible)
		err = store.RenameObject(bucketName, srcKey, dstKey)
		if err != nil {
			t.Fatalf("RenameObject with same content at destination failed: %v", err)
		}

		// Source should be deleted
		_, _, err = store.GetObject(bucketName, srcKey)
		if err != ErrObjectNotFound {
			t.Error("Source should be deleted after rename")
		}

		// Destination should still exist
		reader, _, err := store.GetObject(bucketName, dstKey)
		if err != nil {
			t.Fatalf("GetObject destination failed: %v", err)
		}
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Error("Destination should have correct content")
		}
	})

	// Test 2: Rename to existing destination with different content (should overwrite)
	t.Run("DifferentContentAtDestination", func(t *testing.T) {
		srcKey := "rename-src2.txt"
		dstKey := "rename-dst2.txt"
		srcContent := []byte("source content")
		dstContent := []byte("destination content different")

		// Create source
		_, err := store.PutObject(bucketName, srcKey, bytes.NewReader(srcContent), "text/plain")
		if err != nil {
			t.Fatalf("PutObject source failed: %v", err)
		}

		// Create destination with different content
		_, err = store.PutObject(bucketName, dstKey, bytes.NewReader(dstContent), "text/plain")
		if err != nil {
			t.Fatalf("PutObject destination failed: %v", err)
		}

		// Rename - should succeed and overwrite destination
		err = store.RenameObject(bucketName, srcKey, dstKey)
		if err != nil {
			t.Fatalf("RenameObject failed: %v", err)
		}

		// Source should be deleted (rename succeeded)
		_, _, err = store.GetObject(bucketName, srcKey)
		if err != ErrObjectNotFound {
			t.Error("Source should be deleted after rename")
		}

		// Destination should now have source content (overwritten)
		reader, _, err := store.GetObject(bucketName, dstKey)
		if err != nil {
			t.Fatalf("GetObject destination failed: %v", err)
		}
		defer reader.Close()

		dstData, _ := io.ReadAll(reader)
		if !bytes.Equal(dstData, srcContent) {
			t.Error("Destination should have source content after rename (overwrite)")
		}
	})
}

// TestContentAddressableStorageDeduplication tests that duplicate files share the same physical storage
func TestContentAddressableStorageDeduplication(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Create first object with content that exceeds inline threshold
	key1 := "file1.txt"
	content := bytes.Repeat([]byte("duplicate content"), 300) // ~5100 bytes - over threshold

	objInfo1, err := store.PutObject(bucketName, key1, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject for file1 failed: %v", err)
	}

	// Create second object with SAME content
	key2 := "file2.txt"
	objInfo2, err := store.PutObject(bucketName, key2, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject for file2 failed: %v", err)
	}

	// ETags should match since content is the same
	if objInfo1.ETag != objInfo2.ETag {
		t.Errorf("ETags should match for identical content: %s != %s", objInfo1.ETag, objInfo2.ETag)
	}

	// Verify both objects can be read back with correct content
	reader1, _, err := store.GetObject(bucketName, key1)
	if err != nil {
		t.Fatalf("GetObject for file1 failed: %v", err)
	}
	defer reader1.Close()

	data1, err := io.ReadAll(reader1)
	if err != nil {
		t.Fatalf("Failed to read file1: %v", err)
	}

	if !bytes.Equal(data1, content) {
		t.Error("Content doesn't match for file1")
	}

	reader2, _, err := store.GetObject(bucketName, key2)
	if err != nil {
		t.Fatalf("GetObject for file2 failed: %v", err)
	}
	defer reader2.Close()

	data2, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("Failed to read file2: %v", err)
	}

	if !bytes.Equal(data2, content) {
		t.Error("Content doesn't match for file2")
	}

	// Verify that .objects directory exists and contains the deduplicated content
	objectsDir := store.objectsDir
	if _, err := os.Stat(objectsDir); err != nil {
		t.Errorf(".objects directory should exist: %v", err)
	}

	// Count files in .objects directory - should be exactly 1 content file
	// (refcounts are stored in BoltDB, not as separate files)
	fileCount := 0
	err = filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk .objects directory: %v", err)
	}

	if fileCount != 1 {
		t.Errorf("Expected 1 content file in .objects (deduplicated), got %d", fileCount)
	}

	// Test copy also uses the same content-addressed object
	key3 := "file3.txt"
	_, err = store.CopyObject(bucketName, key1, bucketName, key3)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify copied content
	reader3, info3, err := store.GetObject(bucketName, key3)
	if err != nil {
		t.Fatalf("GetObject for file3 failed: %v", err)
	}
	defer reader3.Close()

	data3, err := io.ReadAll(reader3)
	if err != nil {
		t.Fatalf("Failed to read file3: %v", err)
	}

	if !bytes.Equal(data3, content) {
		t.Error("Content doesn't match for file3")
	}

	if info3.ETag != objInfo1.ETag {
		t.Error("ETag should match for copied file")
	}

	// Still should have only 1 content file in .objects (all three files reference the same content)
	// Refcounts are in BoltDB
	fileCount = 0
	err = filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk .objects directory: %v", err)
	}

	if fileCount != 1 {
		t.Errorf("Expected 1 content file in .objects after copy (still deduplicated), got %d", fileCount)
	}
}

// TestContentAddressableStorageWithDifferentContent tests that different files get separate storage
func TestContentAddressableStorageWithDifferentContent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Create first object
	key1 := "file1.txt"
	content1 := bytes.Repeat([]byte("content A"), 500) // ~4500 bytes - over threshold

	objInfo1, err := store.PutObject(bucketName, key1, bytes.NewReader(content1), "text/plain")
	if err != nil {
		t.Fatalf("PutObject for file1 failed: %v", err)
	}

	// Create second object with DIFFERENT content
	key2 := "file2.txt"
	content2 := bytes.Repeat([]byte("content B"), 500) // ~4500 bytes - over threshold

	objInfo2, err := store.PutObject(bucketName, key2, bytes.NewReader(content2), "text/plain")
	if err != nil {
		t.Fatalf("PutObject for file2 failed: %v", err)
	}

	// ETags should be different since content is different
	if objInfo1.ETag == objInfo2.ETag {
		t.Error("ETags should be different for different content")
	}

	// Verify both objects can be read back with correct content
	reader1, _, err := store.GetObject(bucketName, key1)
	if err != nil {
		t.Fatalf("GetObject for file1 failed: %v", err)
	}
	defer reader1.Close()

	data1, err := io.ReadAll(reader1)
	if err != nil {
		t.Fatalf("Failed to read file1: %v", err)
	}

	if !bytes.Equal(data1, content1) {
		t.Error("Content doesn't match for file1")
	}

	reader2, _, err := store.GetObject(bucketName, key2)
	if err != nil {
		t.Fatalf("GetObject for file2 failed: %v", err)
	}
	defer reader2.Close()

	data2, err := io.ReadAll(reader2)
	if err != nil {
		t.Fatalf("Failed to read file2: %v", err)
	}

	if !bytes.Equal(data2, content2) {
		t.Error("Content doesn't match for file2")
	}

	// Count content files in .objects directory - should be 2 content files (different content)
	// Refcounts are in BoltDB
	objectsDir := store.objectsDir
	fileCount := 0
	err = filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk .objects directory: %v", err)
	}

	if fileCount != 2 {
		t.Errorf("Expected 2 content files in .objects (different content), got %d", fileCount)
	}
}

// TestInlineDataNotInObjectsDirectory tests that small inline files are not stored in .objects
func TestInlineDataNotInObjectsDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Create small object that should be inlined
	key := "small.txt"
	content := []byte("small content")

	_, err = store.PutObject(bucketName, key, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify object can be read back
	reader, _, err := store.GetObject(bucketName, key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Error("Content doesn't match")
	}

	// .objects directory should be empty (no files, only directories)
	objectsDir := store.objectsDir
	fileCount := 0
	err = filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk .objects directory: %v", err)
	}

	if fileCount != 0 {
		t.Errorf("Expected 0 files in .objects for inline data, got %d", fileCount)
	}
}

// TestReferenceCountingCleanup tests that content-addressed objects are deleted when no longer referenced
func TestReferenceCountingCleanup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Create content that will be deduplicated
	content := bytes.Repeat([]byte("content for refcount"), 300) // ~6000 bytes

	// Upload same content to 3 different keys
	key1 := "ref1.txt"
	key2 := "ref2.txt"
	key3 := "ref3.txt"

	_, err = store.PutObject(bucketName, key1, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject key1 failed: %v", err)
	}

	_, err = store.PutObject(bucketName, key2, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject key2 failed: %v", err)
	}

	_, err = store.PutObject(bucketName, key3, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject key3 failed: %v", err)
	}

	// Count content files in .objects - should be 1
	objectsDir := store.objectsDir
	countContentFiles := func() int {
		count := 0
		filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				count++
			}
			return nil
		})
		return count
	}

	if countContentFiles() != 1 {
		t.Errorf("Expected 1 content file after 3 uploads, got %d", countContentFiles())
	}

	// Delete first object - content should still exist
	err = store.DeleteObject(bucketName, key1)
	if err != nil {
		t.Fatalf("DeleteObject key1 failed: %v", err)
	}

	if countContentFiles() != 1 {
		t.Errorf("Expected 1 content file after deleting 1 of 3 references, got %d", countContentFiles())
	}

	// Verify key2 and key3 still work
	reader, _, err := store.GetObject(bucketName, key2)
	if err != nil {
		t.Fatalf("GetObject key2 failed after deleting key1: %v", err)
	}
	reader.Close()

	// Delete second object - content should still exist
	err = store.DeleteObject(bucketName, key2)
	if err != nil {
		t.Fatalf("DeleteObject key2 failed: %v", err)
	}

	if countContentFiles() != 1 {
		t.Errorf("Expected 1 content file after deleting 2 of 3 references, got %d", countContentFiles())
	}

	// Delete third object - content should now be deleted
	err = store.DeleteObject(bucketName, key3)
	if err != nil {
		t.Fatalf("DeleteObject key3 failed: %v", err)
	}

	if countContentFiles() != 0 {
		t.Errorf("Expected 0 content files after deleting all references, got %d", countContentFiles())
	}
}

// TestReferenceCountingWithCopy tests reference counting with copy operations
func TestReferenceCountingWithCopy(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	// Create content
	content := bytes.Repeat([]byte("copy test content"), 300) // ~5100 bytes

	// Upload original
	key1 := "original.txt"
	_, err = store.PutObject(bucketName, key1, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy to another key
	key2 := "copy.txt"
	_, err = store.CopyObject(bucketName, key1, bucketName, key2)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Count content files - should be 1 (shared)
	objectsDir := store.objectsDir
	countContentFiles := func() int {
		count := 0
		filepath.Walk(objectsDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				count++
			}
			return nil
		})
		return count
	}

	if countContentFiles() != 1 {
		t.Errorf("Expected 1 content file after copy, got %d", countContentFiles())
	}

	// Delete original - copy should still work
	err = store.DeleteObject(bucketName, key1)
	if err != nil {
		t.Fatalf("DeleteObject original failed: %v", err)
	}

	// Verify copy still works
	reader, _, err := store.GetObject(bucketName, key2)
	if err != nil {
		t.Fatalf("GetObject copy failed after deleting original: %v", err)
	}
	data, _ := io.ReadAll(reader)
	reader.Close()

	if !bytes.Equal(data, content) {
		t.Error("Copy content doesn't match after deleting original")
	}

	if countContentFiles() != 1 {
		t.Errorf("Expected 1 content file after deleting original, got %d", countContentFiles())
	}

	// Delete copy - content should now be deleted
	err = store.DeleteObject(bucketName, key2)
	if err != nil {
		t.Fatalf("DeleteObject copy failed: %v", err)
	}

	if countContentFiles() != 0 {
		t.Errorf("Expected 0 content files after deleting all references, got %d", countContentFiles())
	}
}
