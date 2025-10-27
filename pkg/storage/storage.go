package storage

import (
	"encoding/gob"
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
	tempDir    = ".temp"
	// inlineThreshold is the maximum size (in bytes) for files to be stored inline in metadata
	// Files smaller than or equal to this size will be embedded in the meta file
	inlineThreshold = 4096
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
	Key         string
	Size        int64
	ETag        string
	ModTime     time.Time
	ContentType string
}

// BucketInfo contains metadata about a bucket
type BucketInfo struct {
	Name    string
	ModTime time.Time
}

// Multipart represents a part of a multipart upload
type Multipart struct {
	PartNumber int
	ETag       string
}

// Part represents a stored part of list parts
type Part struct {
	PartNumber int
	ETag       string
	Size       int64
	ModTime    time.Time
}

// MultipartUpload represents an in-progress multipart upload
type MultipartUpload struct {
	UploadID string
	Bucket   string
	Key      string
	ModTime  time.Time
}

// Storage is the local filesystem storage backend
type Storage struct {
	basePath string
	tempDir  string
}

// NewStorage creates a new local storage backend
func NewStorage(basePath string) (*Storage, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, err
	}

	tempDir := filepath.Join(absPath, tempDir)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, err
	}

	s := &Storage{
		basePath: absPath,
		tempDir:  tempDir,
	}

	return s, nil
}

func (s *Storage) tempFile() (*os.File, error) {
	return os.CreateTemp(s.tempDir, "tmp-*")
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

// safePath returns the safe filesystem path for an object
// Returns the object directory path (not the data file)
func (s *Storage) safePath(bucket, key string) (string, error) {
	if err := sanitizeBucketName(bucket); err != nil {
		return "", err
	}

	bucketPath := filepath.Join(s.basePath, bucket)

	if key == "" {
		return bucketPath, nil
	}

	if err := sanitizeObjectKey(key); err != nil {
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

// ObjectMetadata represents object metadata
type ObjectMetadata struct {
	ContentType string
	ETag        string
	// Data stores the file content inline for small files (<=256 bytes)
	// If Data is not nil and not empty, it contains the entire file content
	Data []byte
}

// UploadMetadata represents multipart upload metadata
type UploadMetadata struct {
	ContentType string
}

// saveObjectMetadata saves object metadata
func (s *Storage) saveObjectMetadata(path string, metadata *ObjectMetadata) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadata)
}

// loadObjectMetadata loads object metadata
func (s *Storage) loadObjectMetadata(path string) (*ObjectMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var metadata ObjectMetadata
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// saveUploadMetadata saves upload metadata
func (s *Storage) saveUploadMetadata(path string, metadata *UploadMetadata) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadata)
}

// loadUploadMetadata loads upload metadata
func (s *Storage) loadUploadMetadata(path string) (*UploadMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var metadata UploadMetadata
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// cleanupEmptyDirs removes empty parent directories up to but not including the stopDir
// This function is best-effort and will not fail the operation if cleanup fails
func (s *Storage) cleanupEmptyDirs(dir, stopDir string) {
	// Make sure both paths are absolute for comparison
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return
	}
	absStopDir, err := filepath.Abs(stopDir)
	if err != nil {
		return
	}

	current := absDir
	for {
		// Stop if we've reached the stop directory
		if current == absStopDir {
			break
		}

		// Ensure current is within stopDir using filepath.Rel
		rel, err := filepath.Rel(absStopDir, current)
		if err != nil || strings.HasPrefix(rel, "..") {
			// Current is not within stopDir, stop
			break
		}

		// Try to read the directory
		entries, err := os.ReadDir(current)
		if err != nil {
			// If directory doesn't exist or can't be read, stop
			break
		}

		// If directory is not empty, stop
		if len(entries) > 0 {
			break
		}

		// Directory is empty, remove it
		if err := os.Remove(current); err != nil {
			// If we can't remove it, stop
			break
		}

		// Move to parent directory
		current = filepath.Dir(current)
	}
}
