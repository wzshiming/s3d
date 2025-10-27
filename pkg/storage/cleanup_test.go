package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestDeleteObjectCleansUpEmptyFolders tests that DeleteObject cleans up empty parent directories
func TestDeleteObjectCleansUpEmptyFolders(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	// Create objects with nested paths
	keys := []string{
		"folder1/file1.txt",
		"folder1/subfolder/file2.txt",
		"folder2/file3.txt",
	}

	for _, key := range keys {
		_, err := store.PutObject(bucketName, key, bytes.NewReader([]byte("test content")), "text/plain")
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", key, err)
		}
	}

	bucketPath := filepath.Join(tmpDir, bucketName)

	// Verify initial structure
	if _, err := os.Stat(filepath.Join(bucketPath, "folder1")); err != nil {
		t.Error("folder1 should exist")
	}
	if _, err := os.Stat(filepath.Join(bucketPath, "folder1/subfolder")); err != nil {
		t.Error("folder1/subfolder should exist")
	}

	// Delete nested file
	if err := store.DeleteObject(bucketName, "folder1/subfolder/file2.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// subfolder should be cleaned up (empty)
	if _, err := os.Stat(filepath.Join(bucketPath, "folder1/subfolder")); !os.IsNotExist(err) {
		t.Error("folder1/subfolder should be cleaned up after deleting its only file")
	}

	// folder1 should still exist (contains file1.txt)
	if _, err := os.Stat(filepath.Join(bucketPath, "folder1")); err != nil {
		t.Error("folder1 should still exist")
	}

	// Delete the last file in folder1
	if err := store.DeleteObject(bucketName, "folder1/file1.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// folder1 should now be cleaned up (empty)
	if _, err := os.Stat(filepath.Join(bucketPath, "folder1")); !os.IsNotExist(err) {
		t.Error("folder1 should be cleaned up after deleting its last file")
	}

	// folder2 should still exist
	if _, err := os.Stat(filepath.Join(bucketPath, "folder2")); err != nil {
		t.Error("folder2 should still exist")
	}
}

// TestRenameObjectCleansUpEmptyFolders tests that RenameObject cleans up empty source directories
func TestRenameObjectCleansUpEmptyFolders(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	// Create objects
	_, err = store.PutObject(bucketName, "old-folder/file1.txt", bytes.NewReader([]byte("content")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.PutObject(bucketName, "other-folder/file2.txt", bytes.NewReader([]byte("content")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	bucketPath := filepath.Join(tmpDir, bucketName)

	// Verify old-folder exists
	if _, err := os.Stat(filepath.Join(bucketPath, "old-folder")); err != nil {
		t.Error("old-folder should exist")
	}

	// Rename object to a different folder
	if err := store.RenameObject(bucketName, "old-folder/file1.txt", "new-folder/file1.txt"); err != nil {
		t.Fatalf("RenameObject failed: %v", err)
	}

	// old-folder should be cleaned up (empty)
	if _, err := os.Stat(filepath.Join(bucketPath, "old-folder")); !os.IsNotExist(err) {
		t.Error("old-folder should be cleaned up after renaming its only file")
	}

	// new-folder should exist
	if _, err := os.Stat(filepath.Join(bucketPath, "new-folder")); err != nil {
		t.Error("new-folder should exist")
	}

	// other-folder should still exist
	if _, err := os.Stat(filepath.Join(bucketPath, "other-folder")); err != nil {
		t.Error("other-folder should still exist")
	}
}

// TestRenameObjectWithSameContentCleansUpEmptyFolders tests cleanup when renaming with same content at destination
func TestRenameObjectWithSameContentCleansUpEmptyFolders(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	content := []byte("same content")

	// Create source and destination with same content
	_, err = store.PutObject(bucketName, "src-folder/file.txt", bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.PutObject(bucketName, "dst-folder/file.txt", bytes.NewReader(content), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	bucketPath := filepath.Join(tmpDir, bucketName)

	// Rename - should detect same content and just delete source
	if err := store.RenameObject(bucketName, "src-folder/file.txt", "dst-folder/file.txt"); err != nil {
		t.Fatalf("RenameObject failed: %v", err)
	}

	// src-folder should be cleaned up (empty)
	if _, err := os.Stat(filepath.Join(bucketPath, "src-folder")); !os.IsNotExist(err) {
		t.Error("src-folder should be cleaned up after rename optimization")
	}

	// dst-folder should still exist
	if _, err := os.Stat(filepath.Join(bucketPath, "dst-folder")); err != nil {
		t.Error("dst-folder should still exist")
	}
}

// TestAbortMultipartUploadCleansUpEmptyFolders tests that AbortMultipartUpload cleans up empty directories
func TestAbortMultipartUploadCleansUpEmptyFolders(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	// Initiate multipart upload with nested path
	uploadID, err := store.InitiateMultipartUpload(bucketName, "folder1/subfolder/file.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	uploadsPath := filepath.Join(tmpDir, uploadsDir, bucketName)

	// Verify structure exists
	if _, err := os.Stat(filepath.Join(uploadsPath, "folder1/subfolder/file.txt", uploadID)); err != nil {
		t.Error("Upload directory should exist")
	}

	// Abort upload
	if err := store.AbortMultipartUpload(bucketName, "folder1/subfolder/file.txt", uploadID); err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Empty folders should be cleaned up
	if _, err := os.Stat(filepath.Join(uploadsPath, "folder1/subfolder")); !os.IsNotExist(err) {
		t.Error("folder1/subfolder should be cleaned up")
	}
	if _, err := os.Stat(filepath.Join(uploadsPath, "folder1")); !os.IsNotExist(err) {
		t.Error("folder1 should be cleaned up")
	}
}

// TestCompleteMultipartUploadCleansUpEmptyFolders tests that CompleteMultipartUpload cleans up empty directories
func TestCompleteMultipartUploadCleansUpEmptyFolders(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	// Initiate multipart upload
	uploadID, err := store.InitiateMultipartUpload(bucketName, "folder1/subfolder/file.txt", "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	// Upload a part
	objInfo, err := store.UploadPart(bucketName, "folder1/subfolder/file.txt", uploadID, 1, bytes.NewReader([]byte("test content")))
	if err != nil {
		t.Fatal(err)
	}

	uploadsPath := filepath.Join(tmpDir, uploadsDir, bucketName)

	// Verify upload structure exists
	if _, err := os.Stat(filepath.Join(uploadsPath, "folder1/subfolder/file.txt", uploadID)); err != nil {
		t.Error("Upload directory should exist")
	}

	// Complete upload
	_, err = store.CompleteMultipartUpload(bucketName, "folder1/subfolder/file.txt", uploadID, []Part{{PartNumber: 1, ETag: objInfo.ETag}})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Upload folders should be cleaned up
	if _, err := os.Stat(filepath.Join(uploadsPath, "folder1/subfolder")); !os.IsNotExist(err) {
		t.Error("Upload folder1/subfolder should be cleaned up")
	}
	if _, err := os.Stat(filepath.Join(uploadsPath, "folder1")); !os.IsNotExist(err) {
		t.Error("Upload folder1 should be cleaned up")
	}

	// Verify object was created in the bucket
	bucketPath := filepath.Join(tmpDir, bucketName)
	if _, err := os.Stat(filepath.Join(bucketPath, "folder1/subfolder/file.txt")); err != nil {
		t.Error("Object should be created in bucket")
	}
}

// TestDeleteObjectDoesNotCleanupNonEmptyFolders verifies that non-empty folders are preserved
func TestDeleteObjectDoesNotCleanupNonEmptyFolders(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	// Create multiple objects in the same folder
	_, err = store.PutObject(bucketName, "folder/file1.txt", bytes.NewReader([]byte("content1")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.PutObject(bucketName, "folder/file2.txt", bytes.NewReader([]byte("content2")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	bucketPath := filepath.Join(tmpDir, bucketName)

	// Delete one file
	if err := store.DeleteObject(bucketName, "folder/file1.txt"); err != nil {
		t.Fatal(err)
	}

	// Folder should still exist because it has file2.txt
	if _, err := os.Stat(filepath.Join(bucketPath, "folder")); err != nil {
		t.Error("folder should still exist as it contains another file")
	}

	// Verify file2 still exists
	_, _, err = store.GetObject(bucketName, "folder/file2.txt")
	if err != nil {
		t.Error("file2.txt should still exist")
	}
}

// TestCleanupStopsAtBucketBoundary verifies that cleanup doesn't remove the bucket itself
func TestCleanupStopsAtBucketBoundary(t *testing.T) {
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
	if err := store.CreateBucket(bucketName); err != nil {
		t.Fatal(err)
	}

	// Create and delete a single object at the root of the bucket
	_, err = store.PutObject(bucketName, "file.txt", bytes.NewReader([]byte("content")), "text/plain")
	if err != nil {
		t.Fatal(err)
	}

	if err := store.DeleteObject(bucketName, "file.txt"); err != nil {
		t.Fatal(err)
	}

	// Bucket should still exist even though it's now empty
	if !store.BucketExists(bucketName) {
		t.Error("bucket should still exist after deleting all objects")
	}
}
