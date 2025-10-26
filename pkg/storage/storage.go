package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	dataFile   = "data"
	metaFile   = "meta"
	uploadsDir = ".uploads"
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrObjectNotFound      = errors.New("object not found")
	ErrInvalidUploadID     = errors.New("invalid upload id")
	ErrInvalidPartNumber   = errors.New("invalid part number")
	ErrInvalidBucketName   = errors.New("invalid bucket name")
	ErrInvalidObjectKey    = errors.New("invalid object key")
)

// ObjectInfo contains metadata about an object
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// BucketInfo contains metadata about a bucket
type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

// Part represents a part of a multipart upload
type Part struct {
	PartNumber int
	ETag       string
	Size       int64
}

// MultipartUpload represents an in-progress multipart upload
type MultipartUpload struct {
	UploadID string
	Bucket   string
	Key      string
	Created  time.Time
}

// Storage is the local filesystem storage backend
type Storage struct {
	basePath string
}

// NewStorage creates a new local storage backend
func NewStorage(basePath string) (*Storage, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absPath, 0755); err != nil {
		return nil, err
	}
	s := &Storage{
		basePath: absPath,
	}

	return s, nil
}

// sanitizeBucketName validates and sanitizes bucket name
func sanitizeBucketName(bucket string) error {
	if bucket == "" || bucket == "." || bucket == ".." {
		return ErrInvalidBucketName
	}
	if strings.Contains(bucket, "/") || strings.Contains(bucket, "\\") {
		return ErrInvalidBucketName
	}
	if strings.HasPrefix(bucket, ".") {
		return ErrInvalidBucketName
	}
	return nil
}

// sanitizeBucketName validates and sanitizes bucket name (method version)
func (s *Storage) sanitizeBucketName(bucket string) error {
	return sanitizeBucketName(bucket)
}

// sanitizeObjectKey validates and sanitizes object key
func sanitizeObjectKey(key string) error {
	if key == "" || key == "." || key == ".." {
		return ErrInvalidObjectKey
	}
	// Check for path traversal attempts
	if strings.Contains(key, "..") {
		return ErrInvalidObjectKey
	}
	// Don't allow absolute paths
	if strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") {
		return ErrInvalidObjectKey
	}
	return nil
}

// sanitizeObjectKey validates and sanitizes object key (method version)
func (s *Storage) sanitizeObjectKey(key string) error {
	return sanitizeObjectKey(key)
}

// safePath returns the safe filesystem path for an object
// Returns the object directory path (not the data file)
func (s *Storage) safePath(bucket, key string) (string, error) {
	if err := s.sanitizeBucketName(bucket); err != nil {
		return "", err
	}

	bucketPath := filepath.Join(s.basePath, bucket)

	if key == "" {
		return bucketPath, nil
	}

	if err := s.sanitizeObjectKey(key); err != nil {
		return "", err
	}

	// Object path is now a directory
	objectPath := filepath.Join(bucketPath, key)

	// Verify the path is within the bucket
	absObjectPath, err := filepath.Abs(objectPath)
	if err != nil {
		return "", err
	}

	absBucketPath, err := filepath.Abs(bucketPath)
	if err != nil {
		return "", err
	}

	if !strings.HasPrefix(absObjectPath, absBucketPath+string(filepath.Separator)) {
		return "", ErrInvalidObjectKey
	}

	return objectPath, nil
}

// Metadata represents object metadata
type Metadata struct {
	ContentType string `json:"Content-Type,omitempty"`
	ETag        string `json:"ETag,omitempty"`
}

// saveMetadata saves object metadata
func (s *Storage) saveMetadata(path string, metadata *Metadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// loadMetadata loads object metadata
func (s *Storage) loadMetadata(path string) (*Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}
