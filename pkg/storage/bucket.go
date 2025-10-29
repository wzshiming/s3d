package storage

import (
	"os"
	"path/filepath"
	"strings"
)

// CreateBucket creates a new bucket
func (s *Storage) CreateBucket(bucket string) error {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	if _, err := os.Stat(bucketPath); err == nil {
		return ErrBucketAlreadyExists
	}

	if err := os.MkdirAll(bucketPath, 0755); err != nil {
		return err
	}

	// Create bucket metadata
	metadata := &bucketMetadata{}
	metaPath := filepath.Join(bucketPath, bucketMetaFile)
	if err := saveBucketMetadata(metaPath, metadata); err != nil {
		// Clean up bucket directory if metadata save fails
		os.RemoveAll(bucketPath)
		return err
	}

	return nil
}

// DeleteBucket deletes a bucket
func (s *Storage) DeleteBucket(bucket string) error {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		return ErrBucketNotFound
	}

	return os.RemoveAll(bucketPath)
}

// ListBuckets lists all buckets with pagination support
func (s *Storage) ListBuckets(prefix, continuationToken string, maxBuckets int) ([]BucketInfo, error) {
	entries, err := os.ReadDir(s.basePath)
	if err != nil {
		return nil, err
	}

	var buckets []BucketInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if sanitizeBucketName(name) != nil {
			continue
		}
		
		// Filter by prefix if provided
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		
		// Skip buckets before or equal to continuationToken (for pagination)
		if continuationToken != "" && name <= continuationToken {
			continue
		}
		
		info, err := entry.Info()
		if err != nil {
			continue
		}
		buckets = append(buckets, BucketInfo{
			Name:    name,
			ModTime: info.ModTime(),
		})
		
		// Stop if we've reached maxBuckets (fetch one extra to determine if truncated)
		if maxBuckets > 0 && len(buckets) >= maxBuckets {
			break
		}
	}
	return buckets, nil
}

// BucketExists checks if a bucket exists
func (s *Storage) BucketExists(bucket string) bool {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return false
	}

	info, err := os.Stat(bucketPath)
	return err == nil && info.IsDir()
}

// GetRegion returns the configured region for this storage instance
func (s *Storage) GetRegion() string {
	return s.region
}

// GetBucketOwnership returns the ownership control setting for a bucket
func (s *Storage) GetBucketOwnership(bucket string) (string, error) {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return "", err
	}

	if !s.BucketExists(bucket) {
		return "", ErrBucketNotFound
	}

	metaPath := filepath.Join(bucketPath, bucketMetaFile)
	metadata, err := loadBucketMetadata(metaPath)
	if err != nil {
		return "", err
	}

	if metadata == nil || metadata.Ownership == "" {
		// Default to BucketOwnerEnforced if not set
		return "BucketOwnerEnforced", nil
	}

	return metadata.Ownership, nil
}

// PutBucketOwnership sets the ownership control setting for a bucket
func (s *Storage) PutBucketOwnership(bucket, ownership string) error {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	metaPath := filepath.Join(bucketPath, bucketMetaFile)
	metadata, err := loadBucketMetadata(metaPath)
	if err != nil {
		return err
	}

	if metadata == nil {
		metadata = &bucketMetadata{}
	}

	metadata.Ownership = ownership
	return saveBucketMetadata(metaPath, metadata)
}

// DeleteBucketOwnership deletes the ownership control setting for a bucket
func (s *Storage) DeleteBucketOwnership(bucket string) error {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	metaPath := filepath.Join(bucketPath, bucketMetaFile)
	metadata, err := loadBucketMetadata(metaPath)
	if err != nil {
		return err
	}

	if metadata == nil {
		metadata = &bucketMetadata{}
	}

	metadata.Ownership = ""
	return saveBucketMetadata(metaPath, metadata)
}
