package storage

import (
	"os"
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

	return os.MkdirAll(bucketPath, 0755)
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
