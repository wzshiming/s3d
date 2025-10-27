package storage

import (
	"encoding/gob"
	"errors"
	"net/url"
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

// encodePathComponent encodes a path component for filesystem storage
// This handles non-ASCII characters by URL encoding them to ensure
// filesystem compatibility across different platforms
func encodePathComponent(component string) string {
	// URL encode the component, but preserve forward slashes for path separators
	// We need to encode each path segment separately to preserve the directory structure
	return url.PathEscape(component)
}

// decodePathComponent decodes a filesystem path component back to the original key
func decodePathComponent(component string) (string, error) {
	return url.PathUnescape(component)
}

// encodeObjectKey encodes an object key for filesystem storage
// It encodes each path component separately to preserve directory structure
func encodeObjectKey(key string) string {
	if key == "" {
		return ""
	}
	
	// Split the key into path components
	parts := strings.Split(key, "/")
	encodedParts := make([]string, len(parts))
	
	for i, part := range parts {
		encodedParts[i] = encodePathComponent(part)
	}
	
	return strings.Join(encodedParts, "/")
}

// decodeObjectKey decodes a filesystem path back to the original object key
func decodeObjectKey(encodedKey string) (string, error) {
	if encodedKey == "" {
		return "", nil
	}
	
	// Split the encoded key into path components
	parts := strings.Split(encodedKey, "/")
	decodedParts := make([]string, len(parts))
	
	for i, part := range parts {
		decoded, err := decodePathComponent(part)
		if err != nil {
			return "", err
		}
		decodedParts[i] = decoded
	}
	
	return strings.Join(decodedParts, "/"), nil
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

	// Encode the object key for filesystem storage
	encodedKey := encodeObjectKey(key)

	// Object path is now a directory
	objectPath := filepath.Join(bucketPath, filepath.FromSlash(encodedKey))

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
	ContentType string
	ETag        string
	// Data stores the file content inline for small files (<=256 bytes)
	// If Data is not nil and not empty, it contains the entire file content
	Data []byte
}

// saveMetadata saves object metadata
func (s *Storage) saveMetadata(path string, metadata *Metadata) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadata)
}

// loadMetadata loads object metadata
func (s *Storage) loadMetadata(path string) (*Metadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var metadata Metadata
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}
