package storage

import (
	"os"
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

// ListBuckets lists all buckets
func (s *Storage) ListBuckets() ([]BucketInfo, error) {
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
		info, err := entry.Info()
		if err != nil {
			continue
		}
		buckets = append(buckets, BucketInfo{
			Name:    name,
			ModTime: info.ModTime(),
		})
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

// GetBucketLogging retrieves the logging configuration for a bucket
func (s *Storage) GetBucketLogging(bucket string) (*BucketLoggingConfig, error) {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return nil, err
	}

	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	loggingFile := bucketPath + "/.logging"
	config, err := loadBucketLogging(loggingFile)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// PutBucketLogging sets the logging configuration for a bucket
func (s *Storage) PutBucketLogging(bucket string, config *BucketLoggingConfig) error {
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	loggingFile := bucketPath + "/.logging"
	if config == nil {
		// Delete logging configuration
		return os.Remove(loggingFile)
	}

	return saveBucketLogging(loggingFile, config)
}
