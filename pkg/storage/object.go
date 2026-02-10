package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// inlineDataReader wraps a bytes.Reader to implement io.ReadSeekCloser
type inlineDataReader struct {
	*bytes.Reader
}

// Close implements io.Closer (no-op for in-memory data)
func (r *inlineDataReader) Close() error {
	return nil
}

type emptyContent struct{}

func (emptyContent) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (emptyContent) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset != 0 {
			return 0, fmt.Errorf("invalid seek offset for empty content")
		}
		return 0, nil
	case io.SeekCurrent, io.SeekEnd:
		if offset != 0 {
			return 0, fmt.Errorf("invalid seek offset for empty content")
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("invalid whence value")
	}
}

func (emptyContent) Close() error {
	return nil
}

func (s *Storage) objectsPath(oid string) string {
	return filepath.Join(s.objectsDir, oid[:2], oid)
}

func (s *Storage) getObjectMetadata(bucket, key string) (*objectMetadata, error) {
	var metadata objectMetadata
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		val := b.Get([]byte(key))
		if val == nil {
			return ErrObjectNotFound
		}
		decoder := gob.NewDecoder(bytes.NewReader(val))
		if err := decoder.Decode(&metadata); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &metadata, nil
}

func (s *Storage) putObjectMetadata(bucket, key string, metadata *objectMetadata) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(metadata)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		return b.Put([]byte(key), buf.Bytes())
	})
}

func (s *Storage) deleteObjectMetadata(bucket, key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		return b.Delete([]byte(key))
	})
}

// PutObject stores an object
// If expectedChecksumSHA256 is provided (non-empty), it validates the checksum after computing.
func (s *Storage) PutObject(bucket, key string, data io.Reader, userMetadata Metadata, expectedChecksumSHA256 string) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	existingMetadata, _ := s.getObjectMetadata(bucket, key)

	// Create temp file in the object directory
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate SHA256 while writing
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	if _, err := io.Copy(writer, data); err != nil {
		tmpFile.Close()
		return nil, err
	}
	tmpFile.Close()

	// Check file size to determine if it should be inlined
	fileInfo, err := os.Stat(tmpFile.Name())
	if err != nil {
		return nil, err
	}

	sum := hash.Sum(nil)
	etag := hex.EncodeToString(sum)
	checksumSHA256 := base64.StdEncoding.EncodeToString(sum)

	// Validate checksum if provided
	if expectedChecksumSHA256 != "" && expectedChecksumSHA256 != checksumSHA256 {
		return nil, ErrChecksumMismatch
	}

	if existingMetadata != nil && existingMetadata.Etag == etag {
		if !metadataEqual(existingMetadata.Metadata, userMetadata) {
			existingMetadata.Metadata = userMetadata
			err = s.putObjectMetadata(bucket, key, existingMetadata)
			if err != nil {
				return nil, err
			}
		}

		return &ObjectInfo{
			Key:            key,
			Size:           existingMetadata.Size,
			ETag:           existingMetadata.Etag,
			ChecksumSHA256: existingMetadata.Sha256,
			ModTime:        existingMetadata.ModTime,
			Metadata:       existingMetadata.Metadata,
		}, nil
	}

	metadata := &objectMetadata{
		Etag:     etag,
		Sha256:   checksumSHA256,
		Metadata: userMetadata,
		ModTime:  time.Now(),
		Size:     fileInfo.Size(),
	}

	// If file is small enough, embed it in metadata
	if fileInfo.Size() <= inlineThreshold {
		// Read the file content
		fileData, err := os.ReadFile(tmpFile.Name())
		if err != nil {
			return nil, err
		}
		metadata.Data = fileData

		// Save metadata with inline data
		if err := s.putObjectMetadata(bucket, key, metadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest
		if existingMetadata != nil && existingMetadata.Etag != "" {
			s.decrementRefCount(existingMetadata.Etag)
		}
	} else {
		// Store the file in .objects directory
		if err := s.storeContentAddressedObject(tmpFile.Name(), etag); err != nil {
			return nil, err
		}

		// Store metadata with digest reference
		if err := s.putObjectMetadata(bucket, key, metadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest and it's different
		if existingMetadata != nil && existingMetadata.Etag != "" && existingMetadata.Etag != etag {
			s.decrementRefCount(existingMetadata.Etag)
		}
	}

	return &ObjectInfo{
		Key:            key,
		Size:           metadata.Size,
		ETag:           etag,
		ChecksumSHA256: checksumSHA256,
		ModTime:        metadata.ModTime,
		Metadata:       userMetadata,
	}, nil
}

// GetObject retrieves an object
func (s *Storage) GetObject(bucket, key string) (func() (io.ReadSeekCloser, error), *ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, nil, ErrBucketNotFound
	}

	metadata, err := s.getObjectMetadata(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	info := &ObjectInfo{
		Key:            key,
		Size:           metadata.Size,
		ETag:           metadata.Etag,
		ChecksumSHA256: metadata.Sha256,
		ModTime:        metadata.ModTime,
		Metadata:       metadata.Metadata,
	}

	// Check if data is stored inline in metadata
	if len(metadata.Data) > 0 {
		return func() (io.ReadSeekCloser, error) {
			return &inlineDataReader{bytes.NewReader(metadata.Data)}, nil
		}, info, nil
	}

	if metadata.Size == 0 {
		return func() (io.ReadSeekCloser, error) {
			return emptyContent{}, nil
		}, info, nil
	}

	return func() (io.ReadSeekCloser, error) {
		return s.getContentAddressedObject(metadata.Etag)
	}, info, nil
}

// DeleteObject deletes an object
func (s *Storage) DeleteObject(bucket, key string) error {
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	metadata, err := s.getObjectMetadata(bucket, key)
	if err != nil {
		return err
	}

	// Load metadata to check if we need to decrement refcount
	if metadata != nil && metadata.Etag != "" {
		// Decrement reference count for content-addressed object
		if err := s.decrementRefCount(metadata.Etag); err != nil {
			// Log error but don't fail the delete operation
			// The object metadata will be deleted anyway
		}
	}

	if err := s.deleteObjectMetadata(bucket, key); err != nil {
		return err
	}

	return nil
}

func (s *Storage) walkObjects(bucket string, prefix, delimiter, marker string, maxKeys int,
	walkObjectFunc func(key string, metadata *objectMetadata) error,
	walkCommonPrefixFunc func(commonPrefix string) error,
) (nextMarker string, err error) {
	var handledKeys int

	commonPrefixes := map[string]struct{}{}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		c := b.Cursor()
		prefixBytes := []byte(prefix)
		for k, v := c.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = c.Next() {
			key := string(k)
			if marker != "" && key <= marker {
				continue
			}

			if delimiter != "" {
				remainingKey := strings.TrimPrefix(key, prefix)
				if idx := strings.Index(remainingKey, delimiter); idx != -1 {
					commonPrefix := prefix + remainingKey[:idx+len(delimiter)]
					if _, exists := commonPrefixes[commonPrefix]; !exists {
						commonPrefixes[commonPrefix] = struct{}{}
						if err := walkCommonPrefixFunc(commonPrefix); err != nil {
							return err
						}
					}
					continue
				}
			}

			var metadata objectMetadata
			decoder := gob.NewDecoder(bytes.NewReader(v))
			if err := decoder.Decode(&metadata); err != nil {
				return err
			}

			if err := walkObjectFunc(key, &metadata); err != nil {
				return err
			}

			handledKeys++
			if maxKeys > 0 && handledKeys >= maxKeys {
				nextMarker = key
				break
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return nextMarker, nil
}

// ListObjects lists objects in a bucket with optional prefix, delimiter, and marker for pagination
func (s *Storage) ListObjects(bucket, prefix, delimiter, marker string, maxKeys int) (objects []ObjectInfo, prefixes []string, nextContinuationToken string, err error) {
	if !s.BucketExists(bucket) {
		return nil, nil, "", ErrBucketNotFound
	}

	nextContinuationToken, err = s.walkObjects(bucket, prefix, delimiter, marker, maxKeys,
		func(key string, metadata *objectMetadata) error {

			objects = append(objects, ObjectInfo{
				Key:            key,
				Size:           metadata.Size,
				ETag:           metadata.Etag,
				ChecksumSHA256: metadata.Sha256,
				ModTime:        metadata.ModTime,
				Metadata:       metadata.Metadata,
			})
			return nil
		},
		func(commonPrefix string) error {
			prefixes = append(prefixes, commonPrefix)
			return nil
		})
	if err != nil {
		return nil, nil, "", err
	}

	return objects, prefixes, nextContinuationToken, nil
}

// CopyObject copies an object from one location to another
func (s *Storage) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, replaceMetadata *Metadata) (*ObjectInfo, error) {
	// Verify source bucket exists
	if !s.BucketExists(srcBucket) {
		return nil, ErrBucketNotFound
	}

	// Verify destination bucket exists
	if !s.BucketExists(dstBucket) {
		return nil, ErrBucketNotFound
	}

	// Get source object metadata
	srcMetadata, err := s.getObjectMetadata(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}

	existingDstMetadata, _ := s.getObjectMetadata(dstBucket, dstKey)

	// Determine which metadata to use
	var metadataToUse Metadata
	if replaceMetadata != nil {
		metadataToUse = *replaceMetadata
	} else {
		metadataToUse = srcMetadata.Metadata
	}

	if existingDstMetadata != nil && existingDstMetadata.Etag == srcMetadata.Etag && metadataEqual(existingDstMetadata.Metadata, metadataToUse) {
		return &ObjectInfo{
			Key:            dstKey,
			Size:           existingDstMetadata.Size,
			ETag:           existingDstMetadata.Etag,
			ChecksumSHA256: existingDstMetadata.Sha256,
			ModTime:        existingDstMetadata.ModTime,
			Metadata:       existingDstMetadata.Metadata,
		}, nil
	}

	// Check if source data is inline
	if len(srcMetadata.Data) > 0 {
		// Data is inline - copy directly
		dstMetadata := &objectMetadata{
			Size:     srcMetadata.Size,
			Etag:     srcMetadata.Etag,
			Sha256:   srcMetadata.Sha256,
			Metadata: metadataToUse,
			Data:     srcMetadata.Data,
			ModTime:  srcMetadata.ModTime,
		}
		copy(dstMetadata.Data, srcMetadata.Data)

		if err := s.putObjectMetadata(dstBucket, dstKey, dstMetadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest
		if existingDstMetadata != nil && existingDstMetadata.Etag != "" {
			s.decrementRefCount(existingDstMetadata.Etag)
		}

		return &ObjectInfo{
			Key:            dstKey,
			Size:           dstMetadata.Size,
			ETag:           dstMetadata.Etag,
			ChecksumSHA256: dstMetadata.Sha256,
			ModTime:        dstMetadata.ModTime,
			Metadata:       dstMetadata.Metadata,
		}, nil
	}

	if srcMetadata.Size == 0 {
		// Source object has no data (size 0) - create new metadata with no data reference
		dstMetadata := &objectMetadata{
			Size:     0,
			Etag:     srcMetadata.Etag,
			Sha256:   srcMetadata.Sha256,
			Metadata: metadataToUse,
			ModTime:  srcMetadata.ModTime,
		}

		if err := s.putObjectMetadata(dstBucket, dstKey, dstMetadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest
		if existingDstMetadata != nil && existingDstMetadata.Etag != "" {
			s.decrementRefCount(existingDstMetadata.Etag)
		}

		return &ObjectInfo{
			Key:            dstKey,
			Size:           dstMetadata.Size,
			ETag:           dstMetadata.Etag,
			ChecksumSHA256: dstMetadata.Sha256,
			ModTime:        dstMetadata.ModTime,
			Metadata:       dstMetadata.Metadata,
		}, nil
	}

	// Data is in .objects - increment refcount first, then copy the digest reference
	if err := s.incrementRefCount(srcMetadata.Etag); err != nil {
		return nil, err
	}

	dstMetadata := &objectMetadata{
		Size:     srcMetadata.Size,
		Etag:     srcMetadata.Etag,
		Sha256:   srcMetadata.Sha256,
		Metadata: metadataToUse,
		ModTime:  srcMetadata.ModTime,
	}

	if err := s.putObjectMetadata(dstBucket, dstKey, dstMetadata); err != nil {
		return nil, err
	}

	// Decrement refcount for old destination if it had a digest
	if existingDstMetadata != nil && existingDstMetadata.Etag != "" {
		s.decrementRefCount(existingDstMetadata.Etag)
	}

	return &ObjectInfo{
		Key:            dstKey,
		Size:           dstMetadata.Size,
		ETag:           dstMetadata.Etag,
		ChecksumSHA256: dstMetadata.Sha256,
		ModTime:        dstMetadata.ModTime,
		Metadata:       metadataToUse,
	}, nil
}

// RenameObject renames an object within the same bucket
func (s *Storage) RenameObject(bucket, srcKey, dstKey string) error {
	// Verify bucket exists
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	// Get source object metadata
	srcMetadata, err := s.getObjectMetadata(bucket, srcKey)
	if err != nil {
		return err
	}

	existingDstMetadata, _ := s.getObjectMetadata(bucket, dstKey)

	// Store metadata for destination object
	if err := s.putObjectMetadata(bucket, dstKey, srcMetadata); err != nil {
		return err
	}

	// Decrement refcount for old destination if it had a digest and it's different
	if existingDstMetadata != nil && existingDstMetadata.Etag != "" && existingDstMetadata.Etag != srcMetadata.Etag {
		s.decrementRefCount(existingDstMetadata.Etag)
	}

	// Delete source object metadata
	if err := s.deleteObjectMetadata(bucket, srcKey); err != nil {
		return err
	}

	return nil
}
