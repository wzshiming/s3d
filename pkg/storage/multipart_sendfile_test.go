package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"testing"
)

// TestMultipartUploadLargeFiles tests the multipart upload with larger files
// to ensure the sendfile optimization works correctly for substantial data.
func TestMultipartUploadLargeFiles(t *testing.T) {
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

	bucketName := "test-bucket-large"
	objectKey := "large-multipart-object.bin"

	// Create bucket
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	uploadID, err := store.InitiateMultipartUpload(bucketName, objectKey, "application/octet-stream")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Create parts with larger data (1MB each)
	partSize := 1024 * 1024 // 1MB
	numParts := 3
	
	// Calculate expected hash
	expectedHash := sha256.New()
	var parts []Multipart
	
	for i := 1; i <= numParts; i++ {
		// Create deterministic data for each part
		partData := bytes.Repeat([]byte{byte(i)}, partSize)
		expectedHash.Write(partData)
		
		objInfo, err := store.UploadPart(bucketName, objectKey, uploadID, i, bytes.NewReader(partData))
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", i, err)
		}
		
		parts = append(parts, Multipart{
			PartNumber: i,
			ETag:       objInfo.ETag,
		})
	}

	// Complete multipart upload
	finalObjInfo, err := store.CompleteMultipartUpload(bucketName, objectKey, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	if finalObjInfo.ETag == "" {
		t.Fatal("Final ETag should not be empty")
	}

	// Verify the object
	reader, info, err := store.GetObject(bucketName, objectKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	// Verify size
	expectedSize := int64(partSize * numParts)
	if info.Size != expectedSize {
		t.Fatalf("Expected size %d, got %d", expectedSize, info.Size)
	}

	// Verify hash by reading the object and computing hash
	actualHash := sha256.New()
	if _, err := io.Copy(actualHash, reader); err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}

	expectedETag := base64.URLEncoding.EncodeToString(expectedHash.Sum(nil))
	actualETag := base64.URLEncoding.EncodeToString(actualHash.Sum(nil))
	
	if actualETag != expectedETag {
		t.Fatalf("Hash mismatch: expected %s, got %s", expectedETag, actualETag)
	}
	
	// Verify the stored ETag matches
	if finalObjInfo.ETag != expectedETag {
		t.Fatalf("Stored ETag mismatch: expected %s, got %s", expectedETag, finalObjInfo.ETag)
	}
}

// TestMultipartUploadEmptyPart tests handling of empty parts
func TestMultipartUploadEmptyPart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	bucketName := "test-bucket-empty-part"
	objectKey := "empty-part.txt"

	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	uploadID, err := store.InitiateMultipartUpload(bucketName, objectKey, "")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Upload parts including an empty one
	objInfo1, err := store.UploadPart(bucketName, objectKey, uploadID, 1, bytes.NewReader([]byte("before")))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	objInfo2, err := store.UploadPart(bucketName, objectKey, uploadID, 2, bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("UploadPart 2 (empty) failed: %v", err)
	}

	objInfo3, err := store.UploadPart(bucketName, objectKey, uploadID, 3, bytes.NewReader([]byte("after")))
	if err != nil {
		t.Fatalf("UploadPart 3 failed: %v", err)
	}

	parts := []Multipart{
		{PartNumber: 1, ETag: objInfo1.ETag},
		{PartNumber: 2, ETag: objInfo2.ETag},
		{PartNumber: 3, ETag: objInfo3.ETag},
	}

	finalObjInfo, err := store.CompleteMultipartUpload(bucketName, objectKey, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
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

	expectedContent := "beforeafter"
	if string(data) != expectedContent {
		t.Fatalf("Expected %q, got %q", expectedContent, string(data))
	}

	if info.Size != int64(len(expectedContent)) {
		t.Fatalf("Expected size %d, got %d", len(expectedContent), info.Size)
	}

	// Verify hash
	expectedHash := sha256.New()
	expectedHash.Write([]byte(expectedContent))
	expectedETag := base64.URLEncoding.EncodeToString(expectedHash.Sum(nil))

	if finalObjInfo.ETag != expectedETag {
		t.Fatalf("ETag mismatch: expected %s, got %s", expectedETag, finalObjInfo.ETag)
	}
}
