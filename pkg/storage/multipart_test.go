package storage

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestMultipartUpload(t *testing.T) {
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

	bucketName := "test-bucket-multipart"
	objectKey := "multipart-object.txt"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	uploadID, err := store.InitiateMultipartUpload(bucketName, objectKey, "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	if uploadID == "" {
		t.Fatal("Upload ID should not be empty")
	}

	// Upload parts
	part1Content := "Part 1 content"
	part2Content := "Part 2 content"

	objInfo1, err := store.UploadPart(bucketName, objectKey, uploadID, 1, bytes.NewReader([]byte(part1Content)))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	objInfo2, err := store.UploadPart(bucketName, objectKey, uploadID, 2, bytes.NewReader([]byte(part2Content)))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	// Complete multipart upload
	parts := []Multipart{
		{PartNumber: 1, ETag: objInfo1.ETag},
		{PartNumber: 2, ETag: objInfo2.ETag},
	}

	finalObjInfo, err := store.CompleteMultipartUpload(bucketName, objectKey, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	if finalObjInfo.ETag == "" {
		t.Fatal("Final ETag should not be empty")
	}

	// Verify object
	reader, info, err := store.GetObject(bucketName, objectKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}

	expectedContent := part1Content + part2Content
	if string(data) != expectedContent {
		t.Fatalf("Expected %q, got %q", expectedContent, string(data))
	}

	if info.Size != int64(len(expectedContent)) {
		t.Fatalf("Expected size %d, got %d", len(expectedContent), info.Size)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
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

	bucketName := "test-bucket-abort"
	objectKey := "abort-multipart.txt"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	uploadID, err := store.InitiateMultipartUpload(bucketName, objectKey, "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Upload a part
	_, err = store.UploadPart(bucketName, objectKey, uploadID, 1, bytes.NewReader([]byte("test")))
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort upload
	err = store.AbortMultipartUpload(bucketName, objectKey, uploadID)
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify upload is aborted
	_, err = store.UploadPart(bucketName, objectKey, uploadID, 2, bytes.NewReader([]byte("test")))
	if err != ErrInvalidUploadID {
		t.Fatal("Expected ErrInvalidUploadID after abort")
	}
}

func TestListMultipartUploads(t *testing.T) {
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

	bucketName := "test-bucket-list-uploads"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multiple uploads
	uploadID1, err := store.InitiateMultipartUpload(bucketName, "file1.txt", "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	uploadID2, err := store.InitiateMultipartUpload(bucketName, "file2.txt", "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	uploadID3, err := store.InitiateMultipartUpload(bucketName, "prefix/file3.txt", "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// List all uploads
	uploads, err := store.ListMultipartUploads(bucketName, "", "", "", 0)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	if len(uploads) != 3 {
		t.Fatalf("Expected 3 uploads, got %d", len(uploads))
	}

	// List uploads with prefix
	uploads, err = store.ListMultipartUploads(bucketName, "prefix/", "", "", 0)
	if err != nil {
		t.Fatalf("ListMultipartUploads with prefix failed: %v", err)
	}

	if len(uploads) != 1 {
		t.Fatalf("Expected 1 upload with prefix, got %d", len(uploads))
	}

	// Clean up
	store.AbortMultipartUpload(bucketName, "file1.txt", uploadID1)
	store.AbortMultipartUpload(bucketName, "file2.txt", uploadID2)
	store.AbortMultipartUpload(bucketName, "prefix/file3.txt", uploadID3)
}

func TestListParts(t *testing.T) {
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

	bucketName := "test-bucket-list-parts"
	objectKey := "test-parts.txt"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate upload
	uploadID, err := store.InitiateMultipartUpload(bucketName, objectKey, "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Upload parts
	_, err = store.UploadPart(bucketName, objectKey, uploadID, 1, bytes.NewReader([]byte("part1")))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	_, err = store.UploadPart(bucketName, objectKey, uploadID, 2, bytes.NewReader([]byte("part2")))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	_, err = store.UploadPart(bucketName, objectKey, uploadID, 3, bytes.NewReader([]byte("part3")))
	if err != nil {
		t.Fatalf("UploadPart 3 failed: %v", err)
	}

	// List parts
	parts, err := store.ListParts(bucketName, objectKey, uploadID, 0, 0)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}

	if len(parts) != 3 {
		t.Fatalf("Expected 3 parts, got %d", len(parts))
	}

	// Verify parts are sorted by part number
	for i, part := range parts {
		if part.PartNumber != i+1 {
			t.Fatalf("Expected part number %d, got %d", i+1, part.PartNumber)
		}
	}

	// Clean up
	store.AbortMultipartUpload(bucketName, objectKey, uploadID)
}

func TestInvalidUploadID(t *testing.T) {
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

	// Try to upload part with invalid upload ID
	_, err = store.UploadPart("test-bucket", "key.txt", "invalid-upload-id", 1, bytes.NewReader([]byte("data")))
	if err != ErrInvalidUploadID {
		t.Fatalf("Expected ErrInvalidUploadID, got %v", err)
	}
}

func TestInvalidPartNumber(t *testing.T) {
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

	uploadID, err := store.InitiateMultipartUpload("test-bucket", "key.txt", "")
	if err != nil {
		t.Fatal(err)
	}

	// Test invalid part numbers
	invalidParts := []int{0, -1, 10001}
	for _, partNum := range invalidParts {
		_, err = store.UploadPart("test-bucket", "key.txt", uploadID, partNum, bytes.NewReader([]byte("data")))
		if err != ErrInvalidPartNumber {
			t.Errorf("Part %d should return ErrInvalidPartNumber, got %v", partNum, err)
		}
	}
}

func TestMultipartUploadNonexistentBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.InitiateMultipartUpload("nonexistent", "key.txt", "")
	if err != ErrBucketNotFound {
		t.Fatalf("Expected ErrBucketNotFound, got %v", err)
	}
}

func TestCompleteWithWrongBucketKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.CreateBucket("bucket1"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateBucket("bucket2"); err != nil {
		t.Fatal(err)
	}

	uploadID, err := store.InitiateMultipartUpload("bucket1", "key1.txt", "")
	if err != nil {
		t.Fatal(err)
	}

	// Try to complete with wrong bucket
	_, err = store.CompleteMultipartUpload("bucket2", "key1.txt", uploadID, []Multipart{})
	if err != ErrInvalidUploadID {
		t.Fatalf("Expected ErrInvalidUploadID for wrong bucket, got %v", err)
	}

	// Try to complete with wrong key
	_, err = store.CompleteMultipartUpload("bucket1", "key2.txt", uploadID, []Multipart{})
	if err != ErrInvalidUploadID {
		t.Fatalf("Expected ErrInvalidUploadID for wrong key, got %v", err)
	}
}

func TestMultipartUploadPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store and initiate upload
	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}

	uploadID, err := store.InitiateMultipartUpload("test-bucket", "key.txt", "")
	if err != nil {
		t.Fatal(err)
	}

	// Create a new store instance (simulating server restart)
	store2, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Upload part with the new store - should work if persistence works
	objInfo, err := store2.UploadPart("test-bucket", "key.txt", uploadID, 1, bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Fatalf("Upload should work after restart: %v", err)
	}

	// Complete upload should also work
	_, err = store2.CompleteMultipartUpload("test-bucket", "key.txt", uploadID, []Multipart{{PartNumber: 1, ETag: objInfo.ETag}})
	if err != nil {
		t.Fatalf("Complete should work after restart: %v", err)
	}
}
