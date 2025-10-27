package storage

import (
	"os"
	"testing"
)

func TestBucketOperations(t *testing.T) {
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

	// Check bucket exists
	if !store.BucketExists(bucketName) {
		t.Fatal("Bucket should exist")
	}

	// List buckets
	buckets, err := store.ListBuckets("", 0)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}

	found := false
	for _, b := range buckets {
		if b.Name == bucketName {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Bucket not found in list")
	}

	// Delete bucket
	err = store.DeleteBucket(bucketName)
	if err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	// Check bucket doesn't exist
	if store.BucketExists(bucketName) {
		t.Fatal("Bucket should not exist")
	}
}

func TestBucketDuplicateCreation(t *testing.T) {
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

	// Try to create the same bucket again - should fail
	err = store.CreateBucket(bucketName)
	if err != ErrBucketAlreadyExists {
		t.Fatalf("Expected ErrBucketAlreadyExists, got %v", err)
	}
}

func TestBucketInvalidNames(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	invalidNames := []string{
		"",
		".",
		"..",
		".hidden",
		"bucket/with/slashes",
		"bucket\\with\\backslashes",
	}

	for _, name := range invalidNames {
		err := store.CreateBucket(name)
		if err != ErrInvalidBucketName {
			t.Errorf("CreateBucket(%q) should return ErrInvalidBucketName, got %v", name, err)
		}
	}
}

func TestDeleteNonexistentBucket(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "storage-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	err = store.DeleteBucket("nonexistent")
	if err != ErrBucketNotFound {
		t.Fatalf("Expected ErrBucketNotFound, got %v", err)
	}
}
