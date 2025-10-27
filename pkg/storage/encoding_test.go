package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNonASCIIObjectKeys(t *testing.T) {
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

	// Test with various non-ASCII characters
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
		{
			name:    "Special characters",
			key:     "file with spaces and 中文.txt",
			content: "Special chars content",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Put object
			etag, err := store.PutObject(bucketName, tc.key, bytes.NewReader([]byte(tc.content)), "text/plain")
			if err != nil {
				t.Fatalf("PutObject failed for key %q: %v", tc.key, err)
			}
			if etag == "" {
				t.Fatal("ETag should not be empty")
			}

			// Get object
			reader, info, err := store.GetObject(bucketName, tc.key)
			if err != nil {
				t.Fatalf("GetObject failed for key %q: %v", tc.key, err)
			}
			defer reader.Close()

			retrievedContent, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("Failed to read object: %v", err)
			}

			if string(retrievedContent) != tc.content {
				t.Errorf("Content mismatch for key %q: expected %q, got %q", tc.key, tc.content, string(retrievedContent))
			}

			if info.Key != tc.key {
				t.Errorf("Key mismatch: expected %q, got %q", tc.key, info.Key)
			}

			// Verify the filesystem path is encoded
			objectPath, _ := store.safePath(bucketName, tc.key)
			encodedKey := encodeObjectKey(tc.key)
			expectedPath := filepath.Join(tmpDir, bucketName, filepath.FromSlash(encodedKey))
			if objectPath != expectedPath {
				t.Errorf("Path mismatch: expected %q, got %q", expectedPath, objectPath)
			}

			// Verify the actual filesystem directory exists and is encoded
			if _, err := os.Stat(objectPath); os.IsNotExist(err) {
				t.Errorf("Object directory does not exist at %q", objectPath)
			}
		})
	}

	// List objects and verify all keys are decoded correctly
	objects, _, err := store.ListObjects(bucketName, "", "", "", 100)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	if len(objects) != len(testCases) {
		t.Errorf("Expected %d objects, got %d", len(testCases), len(objects))
	}

	// Create a map of expected keys for verification
	expectedKeys := make(map[string]bool)
	for _, tc := range testCases {
		expectedKeys[tc.key] = true
	}

	for _, obj := range objects {
		if !expectedKeys[obj.Key] {
			t.Errorf("Unexpected key in listing: %q", obj.Key)
		}
	}
}

func TestEncodingDecodingRoundTrip(t *testing.T) {
	testCases := []string{
		"simple.txt",
		"文件.txt",
		"файл.txt",
		"ファイル.txt",
		"αρχείο.txt",
		"file 😀.txt",
		"path/to/文件.txt",
		"文件夹/子文件夹/文件.txt",
		"spaces and 中文.txt",
		"special!@#$%^&*()_+-=[]{}|;':,.<>?.txt",
	}

	for _, original := range testCases {
		t.Run(original, func(t *testing.T) {
			encoded := encodeObjectKey(original)
			decoded, err := decodeObjectKey(encoded)
			if err != nil {
				t.Fatalf("Failed to decode %q: %v", encoded, err)
			}
			if decoded != original {
				t.Errorf("Round-trip failed: original=%q, encoded=%q, decoded=%q", original, encoded, decoded)
			}
		})
	}
}

func TestCopyObjectWithNonASCII(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	srcKey := "源文件.txt"
	dstKey := "目标文件.txt"
	content := []byte("Content with non-ASCII key")

	// Create source object
	_, err = store.PutObject(bucketName, srcKey, bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy object
	etag, err := store.CopyObject(bucketName, srcKey, bucketName, dstKey)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}
	if etag == "" {
		t.Fatal("ETag should not be empty")
	}

	// Verify destination
	reader, info, err := store.GetObject(bucketName, dstKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	retrievedContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(retrievedContent, content) {
		t.Error("Content mismatch after copy")
	}

	if info.Key != dstKey {
		t.Errorf("Key mismatch: expected %q, got %q", dstKey, info.Key)
	}
}

func TestMultipartUploadWithNonASCII(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	key := "多部分上传/文件.txt"
	
	// Initiate multipart upload
	uploadID, err := store.InitiateMultipartUpload(bucketName, key, "text/plain")
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}

	// Upload parts
	part1Content := []byte("Part 1 content")
	part2Content := []byte("Part 2 content")

	etag1, err := store.UploadPart(bucketName, key, uploadID, 1, bytes.NewReader(part1Content))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	etag2, err := store.UploadPart(bucketName, key, uploadID, 2, bytes.NewReader(part2Content))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	// Complete multipart upload
	parts := []Part{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	}

	finalETag, err := store.CompleteMultipartUpload(bucketName, key, uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}
	if finalETag == "" {
		t.Fatal("Final ETag should not be empty")
	}

	// Verify the completed object
	reader, info, err := store.GetObject(bucketName, key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	retrievedContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	expectedContent := append(part1Content, part2Content...)
	if !bytes.Equal(retrievedContent, expectedContent) {
		t.Error("Content mismatch after multipart upload")
	}

	if info.Key != key {
		t.Errorf("Key mismatch: expected %q, got %q", key, info.Key)
	}
}

func TestListMultipartUploadsWithNonASCII(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	bucketName := "test-bucket"
	err = store.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{
		"文件1.txt",
		"文件2.txt",
		"folder/файл.txt",
	}

	uploadIDs := make([]string, len(keys))
	for i, key := range keys {
		uploadID, err := store.InitiateMultipartUpload(bucketName, key, "text/plain")
		if err != nil {
			t.Fatalf("InitiateMultipartUpload failed for key %q: %v", key, err)
		}
		uploadIDs[i] = uploadID
	}

	// List multipart uploads
	uploads, err := store.ListMultipartUploads(bucketName, "", "", "", 100)
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	if len(uploads) != len(keys) {
		t.Errorf("Expected %d uploads, got %d", len(keys), len(uploads))
	}

	// Verify all keys are decoded correctly
	foundKeys := make(map[string]bool)
	for _, upload := range uploads {
		foundKeys[upload.Key] = true
		if upload.Bucket != bucketName {
			t.Errorf("Unexpected bucket: %q", upload.Bucket)
		}
	}

	for _, key := range keys {
		if !foundKeys[key] {
			t.Errorf("Key %q not found in uploads listing", key)
		}
	}

	// Cleanup
	for i, key := range keys {
		err := store.AbortMultipartUpload(bucketName, key, uploadIDs[i])
		if err != nil {
			t.Errorf("AbortMultipartUpload failed for key %q: %v", key, err)
		}
	}
}
