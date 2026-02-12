package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	uploadsDir = ".uploads"
	tempDir    = ".temp"
	objectsDir = ".objects"
	// inlineThreshold is the maximum size (in bytes) for files to be stored inline in metadata
	// Files smaller than or equal to this size will be embedded in the meta file
	inlineThreshold = 1024
)

var (
	// refcountBucket is the BoltDB bucket name for reference counts
	refcountBucket = []byte("refcounts")

	contentBucketPrefix  = "content::"
	uploadsBucketPrefix  = "uploads::"
	metadataBucketPrefix = "metadata::"
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
	ErrInvalidRange        = errors.New("invalid byte range")
	ErrInvalidPart         = errors.New("invalid part")
	ErrUploadNotFound      = errors.New("upload not found")
)

// Storage is the local filesystem storage backend
type Storage struct {
	basePath   string
	tempDir    string
	objectsDir string
	uploadsDir string
	db         *bolt.DB
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

	uploadsDir := filepath.Join(absPath, uploadsDir)
	if err := os.MkdirAll(uploadsDir, 0755); err != nil {
		return nil, err
	}

	// Open BoltDB for reference counting
	dbPath := filepath.Join(absPath, "s3d.db")
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
		uploadsDir: uploadsDir,
		db:         db,
	}

	return s, nil
}

// Close closes the storage backend and releases resources
func (s *Storage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Storage) tempFile() (*os.File, error) {
	return os.CreateTemp(s.tempDir, "tmp-*")
}

// objectMetadata represents object metadata
type objectMetadata struct {
	Metadata Metadata

	Size int64

	Data []byte

	Etag string

	Sha256 string

	Md5 string

	ModTime time.Time
}

// uploadMetadata represents multipart upload metadata
type uploadMetadata struct {
	Metadata Metadata

	Key string

	ModTime time.Time
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
	if a.ContentEncoding != b.ContentEncoding {
		return false
	}
	if a.ContentLanguage != b.ContentLanguage {
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

// incrementRefCount increments the reference count for a content-addressed object using BoltDB
func (s *Storage) incrementRefCount(digest string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
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

	err := s.db.Update(func(tx *bolt.Tx) error {
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
	objPath := s.objectsPath(digest)

	err := os.Remove(objPath)
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
	objPath := s.objectsPath(digest)

	// Check if object already exists
	if _, err := os.Stat(objPath); err == nil {
		// Object already exists, just increment refcount
		return s.incrementRefCount(digest)
	}

	// Create parent directory
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return err
	}

	err := os.Rename(srcPath, objPath)
	if err != nil {
		return err
	}

	// Initialize refcount to 1
	return s.incrementRefCount(digest)
}

// getContentAddressedObject opens a content-addressed object for reading
func (s *Storage) getContentAddressedObject(digest string) (*os.File, error) {
	objPath := s.objectsPath(digest)
	file, err := os.Open(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	return file, nil
}
