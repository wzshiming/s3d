package storage

import (
	"bytes"
	"io"
	"os"
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

	bucketName := "test-bucket"
	objectKey := "test-object.txt"
	objectContent := "Hello, World!"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Put object
	etag, err := store.PutObject(bucketName, objectKey, bytes.NewReader([]byte(objectContent)), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if etag == "" {
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
	etag, err := store.CopyObject(srcBucket, srcKey, dstBucket, dstKey)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	if etag == "" {
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

bucketName := "test-bucket"

// Create bucket
err = store.CreateBucket(bucketName)
if err != nil {
t.Fatalf("CreateBucket failed: %v", err)
}

// Test 1: Small file should be stored inline (<=256 bytes)
smallKey := "small.txt"
smallContent := bytes.Repeat([]byte("x"), 100) // 100 bytes - well under threshold

etag1, err := store.PutObject(bucketName, smallKey, bytes.NewReader(smallContent), "text/plain")
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

if info1.ETag != etag1 {
t.Error("ETag mismatch for small file")
}

// Test 2: Large file should have separate data file (>256 bytes)
largeKey := "large.txt"
largeContent := bytes.Repeat([]byte("y"), 512) // 512 bytes - over threshold

etag2, err := store.PutObject(bucketName, largeKey, bytes.NewReader(largeContent), "text/plain")
if err != nil {
t.Fatalf("PutObject for large file failed: %v", err)
}

// Verify data file exists for large files
objectDir2, _ := store.safePath(bucketName, largeKey)
dataPath2 := objectDir2 + "/data"
if _, err := os.Stat(dataPath2); err != nil {
t.Errorf("Large file should have a separate data file: %v", err)
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

if info2.ETag != etag2 {
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
etagCopy, err := store.CopyObject(bucketName, smallKey, bucketName, copiedKey)
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

if etagCopy != etag1 {
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

bucketName := "test-bucket"
err = store.CreateBucket(bucketName)
if err != nil {
t.Fatalf("CreateBucket failed: %v", err)
}

// Test file exactly at threshold (256 bytes)
atThresholdKey := "at-threshold.txt"
atThresholdContent := bytes.Repeat([]byte("z"), 256)

_, err = store.PutObject(bucketName, atThresholdKey, bytes.NewReader(atThresholdContent), "text/plain")
if err != nil {
t.Fatalf("PutObject failed: %v", err)
}

// File at threshold should be inline
objectDir, _ := store.safePath(bucketName, atThresholdKey)
dataPath := objectDir + "/data"
if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
t.Error("File at threshold (256 bytes) should be stored inline")
}

// Test file just above threshold (257 bytes)
aboveThresholdKey := "above-threshold.txt"
aboveThresholdContent := bytes.Repeat([]byte("w"), 257)

_, err = store.PutObject(bucketName, aboveThresholdKey, bytes.NewReader(aboveThresholdContent), "text/plain")
if err != nil {
t.Fatalf("PutObject failed: %v", err)
}

// File above threshold should have separate data file
objectDir2, _ := store.safePath(bucketName, aboveThresholdKey)
dataPath2 := objectDir2 + "/data"
if _, err := os.Stat(dataPath2); err != nil {
t.Error("File above threshold (257 bytes) should have separate data file")
}
}
