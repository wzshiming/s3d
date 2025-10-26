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
	objects, _, err := store.ListObjects(bucketName, "", "", 0)
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

	_, _, err = store.ListObjects("nonexistent", "", "", 0)
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
srcKey := "source.txt"
targetKey := "renamed.txt"
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
etag, err := store.RenameObject(bucketName, srcKey, targetKey)
if err != nil {
t.Fatalf("RenameObject failed: %v", err)
}

if etag == "" {
t.Fatal("ETag should not be empty")
}

// Verify source object no longer exists
_, _, err = store.GetObject(bucketName, srcKey)
if err != ErrObjectNotFound {
t.Fatalf("Expected ErrObjectNotFound for source key, got %v", err)
}

// Verify target object exists with correct content
reader, info, err := store.GetObject(bucketName, targetKey)
if err != nil {
t.Fatalf("GetObject failed for target key: %v", err)
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

func TestRenameObjectNonexistent(t *testing.T) {
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

_, err = store.RenameObject("test-bucket", "nonexistent.txt", "renamed.txt")
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

_, err = store.RenameObject("nonexistent", "source.txt", "target.txt")
if err != ErrBucketNotFound {
t.Fatalf("Expected ErrBucketNotFound, got %v", err)
}
}
