package storage

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bolt "go.etcd.io/bbolt"
)

const (
	metaFile   = "meta"
	uploadsDir = ".uploads"
	tempDir    = ".temp"
	objectsDir = ".objects"
	refcountDB = "refcount.db"
	// inlineThreshold is the maximum size (in bytes) for files to be stored inline in metadata
	// Files smaller than or equal to this size will be embedded in the meta file
	inlineThreshold = 4096
)

var (
	// refcountBucket is the BoltDB bucket name for reference counts
	refcountBucket = []byte("refcounts")
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrObjectNotFound      = errors.New("object not found")
	ErrInvalidUploadID     = errors.New("invalid upload id")
	ErrInvalidPartNumber   = errors.New("invalid part number")
	ErrInvalidBucketName   = errors.New("invalid bucket name")
	ErrInvalidObjectKey    = errors.New("invalid object key")
	ErrChecksumMismatch    = errors.New("checksum mismatch")
)

// Storage is the local filesystem storage backend
type Storage struct {
	basePath   string
	tempDir    string
	objectsDir string
	refcountDB *bolt.DB
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

	objectsDir := filepath.Join(absPath, objectsDir)
	if err := os.MkdirAll(objectsDir, 0755); err != nil {
		return nil, err
	}

	// Open BoltDB for reference counting
	dbPath := filepath.Join(absPath, refcountDB)
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	// Create bucket for reference counts
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(refcountBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	s := &Storage{
		basePath:   absPath,
		tempDir:    tempDir,
		objectsDir: objectsDir,
		refcountDB: db,
	}

	return s, nil
}

// Close closes the storage backend and releases resources
func (s *Storage) Close() error {
	if s.refcountDB != nil {
		return s.refcountDB.Close()
	}
	return nil
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

// objectMetadata represents object metadata
type objectMetadata struct {
	Metadata Metadata

	ETag string
	// Data stores the file content inline for small files (<=4096 bytes)
	// If Data is not nil and not empty, it contains the entire file content
	Data []byte
	// Digest stores the SHA256 digest (hex-encoded) for content-addressed storage
	// When set, the actual data is stored in .objects/{digest[:2]}/{digest}
	// If Data is set (inline storage), Digest is empty
	Digest string
	// IsDir indicates if the original key had a trailing slash (S3 directory object)
	// When true, the key should be reconstructed with a trailing slash
	IsDir bool
}

// uploadMetadata represents multipart upload metadata
type uploadMetadata struct {
	Metadata Metadata
}

func metadataEqual(a, b Metadata) bool {
	if a.CacheControl != b.CacheControl {
		return false
	}
	if a.ContentDisposition != b.ContentDisposition {
		return false
	}
	if a.ContentType != b.ContentType {
		return false
	}
	if len(a.XAmzMeta) != len(b.XAmzMeta) {
		return false
	}
	for key, valA := range a.XAmzMeta {
		if valB, ok := b.XAmzMeta[key]; !ok || valA != valB {
			return false
		}
	}
	return true
}

// saveObjectMetadata saves object metadata
func saveObjectMetadata(path string, metadata *objectMetadata) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadata)
}

// loadObjectMetadata loads object metadata
func loadObjectMetadata(path string) (*objectMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var metadata objectMetadata
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return &metadata, nil
}

// saveUploadMetadata saves upload metadata
func saveUploadMetadata(path string, metadata *uploadMetadata) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(metadata)
}

// loadUploadMetadata loads upload metadata
func loadUploadMetadata(path string) (*uploadMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var metadata uploadMetadata
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

// objectPath returns the path to the content-addressed object file
func (s *Storage) objectPath(digest string) (string, error) {
	// Validate digest length (SHA256 hex is 64 characters)
	if len(digest) < 2 {
		return "", fmt.Errorf("invalid digest: %s", digest)
	}
	// Use first 2 characters for directory sharding to avoid too many files in one directory
	return filepath.Join(s.objectsDir, digest[:2], digest), nil
}

// incrementRefCount increments the reference count for a content-addressed object using BoltDB
func (s *Storage) incrementRefCount(digest string) error {
	return s.refcountDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(refcountBucket)
		if b == nil {
			return fmt.Errorf("refcount bucket not found")
		}

		key := []byte(digest)

		// Get current count
		var count uint64 = 0
		if data := b.Get(key); data != nil {
			count = binary.BigEndian.Uint64(data)
		}

		// Increment
		count++

		// Store back
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, count)
		return b.Put(key, buf)
	})
}

// decrementRefCount decrements the reference count and deletes the object if count reaches 0
func (s *Storage) decrementRefCount(digest string) error {
	var shouldDelete bool

	err := s.refcountDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(refcountBucket)
		if b == nil {
			return fmt.Errorf("refcount bucket not found")
		}

		key := []byte(digest)

		// Get current count
		data := b.Get(key)
		if data == nil {
			// No refcount entry - this is an inconsistency
			// Log it but don't delete the content to avoid data loss
			// A separate garbage collection mechanism should handle orphaned content
			return fmt.Errorf("refcount entry not found for digest %s", digest)
		}

		count := binary.BigEndian.Uint64(data)

		if count <= 1 {
			// Delete the refcount entry
			shouldDelete = true
			return b.Delete(key)
		}

		// Decrement
		count--
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, count)
		return b.Put(key, buf)
	})

	if err != nil {
		return err
	}

	if shouldDelete {
		return s.deleteContentAddressedObject(digest)
	}

	return nil
}

// deleteContentAddressedObject deletes a content-addressed object
func (s *Storage) deleteContentAddressedObject(digest string) error {
	objPath, err := s.objectPath(digest)
	if err != nil {
		return err
	}
	err = os.Remove(objPath)
	// Ignore "file not found" errors - the desired state is achieved
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// storeContentAddressedObject stores data in the .objects directory using SHA256 digest
// Returns nil error on success
// If the object already exists, it increments the reference count
func (s *Storage) storeContentAddressedObject(srcPath string, digest string) error {
	objPath, err := s.objectPath(digest)
	if err != nil {
		return err
	}

	// Check if object already exists
	if _, err := os.Stat(objPath); err == nil {
		// Object already exists, just increment refcount
		return s.incrementRefCount(digest)
	}

	// Create parent directory
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return err
	}

	err = os.Rename(srcPath, objPath)
	if err != nil {
		return err
	}

	// Initialize refcount to 1
	return s.incrementRefCount(digest)
}

// getContentAddressedObject opens a content-addressed object for reading
func (s *Storage) getContentAddressedObject(digest string) (*os.File, error) {
	objPath, err := s.objectPath(digest)
	if err != nil {
		return nil, err
	}
	return os.Open(objPath)
}
