package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// inlineDataReader wraps a bytes.Reader to implement io.ReadSeekCloser
type inlineDataReader struct {
	*bytes.Reader
}

// Close implements io.Closer (no-op for in-memory data)
func (r *inlineDataReader) Close() error {
	return nil
}

// urlSafeToStdBase64 converts URL-safe base64 encoding to standard base64 encoding.
// This is needed because we use URL-safe base64 (with - and _ characters) for ETags/filenames
// to avoid path separator issues, but AWS SDK expects standard base64 (with + and / characters)
// for checksum headers like x-amz-checksum-sha256.
func urlSafeToStdBase64(urlSafe string) string {
	if urlSafe == "" {
		return ""
	}
	std := strings.ReplaceAll(urlSafe, "-", "+")
	std = strings.ReplaceAll(std, "_", "/")
	return std
}

// PutObject stores an object
func (s *Storage) PutObject(bucket, key string, data io.Reader, userMetadata Metadata) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	objectDir, err := s.safePath(bucket, key)
	if err != nil {
		return nil, err
	}

	// Create object directory
	if err := os.MkdirAll(objectDir, 0755); err != nil {
		return nil, err
	}

	metaPath := filepath.Join(objectDir, metaFile)

	// Check if object already exists and load existing metadata
	var existingMetadata *objectMetadata
	if _, err := os.Stat(metaPath); err == nil {
		existingMetadata, err = loadObjectMetadata(metaPath)
		if err != nil {
			// If metadata is corrupted, treat as if object doesn't exist and overwrite
			existingMetadata = nil
		}
	}

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

	etag := base64.URLEncoding.EncodeToString(hash.Sum(nil))

	// Check compatibility: if object exists with same ETag, it's a duplicate write
	// This is compatible and we can proceed without issue (S3 behavior)
	if existingMetadata != nil && existingMetadata.ETag == etag {
		// Same content - compatible duplicate write, just return existing ObjectInfo
		// No need to rewrite the object
		// Note: tmpFile is already cleaned up by defer

		// Always use meta file's ModTime
		metaFileInfo, err := os.Stat(metaPath)
		if err != nil {
			return nil, err
		}

		// If metadata is different, update it
		if existingMetadata != nil && !metadataEqual(existingMetadata.Metadata, userMetadata) {
			existingMetadata.Metadata = userMetadata
			if err := saveObjectMetadata(metaPath, existingMetadata); err != nil {
				return nil, err
			}
		}

		return &ObjectInfo{
			Key:            key,
			Size:           fileInfo.Size(),
			ETag:           etag,
			ChecksumSHA256: urlSafeToStdBase64(etag),
			ModTime:        metaFileInfo.ModTime(),
			Metadata:       existingMetadata.Metadata,
		}, nil
	}

	metadata := &objectMetadata{
		ETag:     etag,
		Metadata: userMetadata,
		IsDir:    strings.HasSuffix(key, "/"),
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
		if err := saveObjectMetadata(metaPath, metadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest
		if existingMetadata != nil && existingMetadata.Digest != "" {
			s.decrementRefCount(existingMetadata.Digest)
		}
	} else {
		// Use content-addressable storage for larger files
		digest := hex.EncodeToString(hash.Sum(nil))
		metadata.Digest = digest

		// Store the file in .objects directory
		if err := s.storeContentAddressedObject(tmpFile.Name(), digest); err != nil {
			return nil, err
		}

		// Store metadata with digest reference
		if err := saveObjectMetadata(metaPath, metadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest and it's different
		if existingMetadata != nil && existingMetadata.Digest != "" && existingMetadata.Digest != digest {
			s.decrementRefCount(existingMetadata.Digest)
		}
	}

	// Always use meta file's ModTime
	metaFileInfo, err := os.Stat(metaPath)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:            key,
		Size:           fileInfo.Size(),
		ETag:           etag,
		ChecksumSHA256: urlSafeToStdBase64(etag),
		ModTime:        metaFileInfo.ModTime(),
		Metadata:       userMetadata,
	}, nil
}

// GetObject retrieves an object
func (s *Storage) GetObject(bucket, key string) (io.ReadSeekCloser, *ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, nil, ErrBucketNotFound
	}

	objectDir, err := s.safePath(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	metaPath := filepath.Join(objectDir, metaFile)

	// Load metadata first
	metadata, err := loadObjectMetadata(metaPath)
	if err != nil {
		return nil, nil, err
	}
	if metadata == nil {
		return nil, nil, ErrObjectNotFound
	}

	// Check if data is stored inline in metadata
	if len(metadata.Data) > 0 {
		// Data is embedded in metadata
		reader := &inlineDataReader{bytes.NewReader(metadata.Data)}

		// For inline data, we need to get the last modified time from the meta file
		metaFileInfo, err := os.Stat(metaPath)
		if err != nil {
			return nil, nil, err
		}

		info := &ObjectInfo{
			Key:            key,
			Size:           int64(len(metadata.Data)),
			ETag:           metadata.ETag,
			ChecksumSHA256: urlSafeToStdBase64(metadata.ETag),
			ModTime:        metaFileInfo.ModTime(),
			Metadata:       metadata.Metadata,
		}

		return reader, info, nil
	}

	// Check if data is in content-addressable storage
	if metadata.Digest != "" {
		// Data is in .objects directory
		file, err := s.getContentAddressedObject(metadata.Digest)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil, ErrObjectNotFound
			}
			return nil, nil, err
		}

		// Get file info for size
		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, nil, err
		}

		// Always use meta file's ModTime
		metaFileInfo, err := os.Stat(metaPath)
		if err != nil {
			file.Close()
			return nil, nil, err
		}

		info := &ObjectInfo{
			Key:            key,
			Size:           fileInfo.Size(),
			ETag:           metadata.ETag,
			ChecksumSHA256: urlSafeToStdBase64(metadata.ETag),
			ModTime:        metaFileInfo.ModTime(),
			Metadata:       metadata.Metadata,
		}

		return file, info, nil
	}

	// Zero-byte object (including folder objects)
	metaFileInfo, err := os.Stat(metaPath)
	if err != nil {
		return nil, nil, err
	}

	info := &ObjectInfo{
		Key:            key,
		Size:           0,
		ETag:           metadata.ETag,
		ChecksumSHA256: urlSafeToStdBase64(metadata.ETag),
		ModTime:        metaFileInfo.ModTime(),
		Metadata:       metadata.Metadata,
	}
	return &inlineDataReader{bytes.NewReader([]byte{})}, info, nil
}

// DeleteObject deletes an object
func (s *Storage) DeleteObject(bucket, key string) error {
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	objectDir, err := s.safePath(bucket, key)
	if err != nil {
		return err
	}

	// Check if object exists by checking for meta file (which always exists)
	metaPath := filepath.Join(objectDir, metaFile)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return ErrObjectNotFound
	}

	// Load metadata to check if we need to decrement refcount
	metadata, err := loadObjectMetadata(metaPath)
	if err == nil && metadata != nil && metadata.Digest != "" {
		// Decrement reference count for content-addressed object
		if err := s.decrementRefCount(metadata.Digest); err != nil {
			// Log error but don't fail the delete operation
			// The object metadata will be deleted anyway
		}
	}

	// Get bucket path before deleting the object
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	// Remove entire object directory
	if err := os.RemoveAll(objectDir); err != nil {
		return err
	}

	// Clean up empty parent directories
	parentDir := filepath.Dir(objectDir)
	s.cleanupEmptyDirs(parentDir, bucketPath)

	return nil
}

// ListObjects lists objects in a bucket with optional prefix, delimiter, and marker for pagination
func (s *Storage) ListObjects(bucket, prefix, delimiter, marker string, maxKeys int) ([]ObjectInfo, []string, error) {
	if !s.BucketExists(bucket) {
		return nil, nil, ErrBucketNotFound
	}

	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return nil, nil, err
	}

	var objects []ObjectInfo
	commonPrefixes := make(map[string]bool)

	err = filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Skip the bucket directory itself
		if path == bucketPath {
			return nil
		}

		// Check if this is a meta file (all objects have meta files)
		if filepath.Base(path) == metaFile && !info.IsDir() {
			objectDir := filepath.Dir(path)
			objectKey, err := filepath.Rel(bucketPath, objectDir)
			if err != nil {
				return nil
			}
			objectKey = filepath.ToSlash(objectKey)

			// Load metadata first to determine if this is a directory object
			metadata, _ := loadObjectMetadata(path)
			if metadata == nil {
				return nil
			}

			// Reconstruct the original key with trailing slash for directory objects
			if metadata.IsDir {
				objectKey = objectKey + "/"
			}

			// Apply prefix filter
			if prefix != "" && !strings.HasPrefix(objectKey, prefix) {
				return nil
			}

			// Apply marker filter - only include objects after the marker
			if marker != "" && objectKey <= marker {
				return nil
			}

			// Handle delimiter
			if delimiter != "" {
				relativeKey := strings.TrimPrefix(objectKey, prefix)
				if idx := strings.Index(relativeKey, delimiter); idx != -1 {
					// This is a common prefix
					commonPrefix := prefix + relativeKey[:idx+len(delimiter)]
					commonPrefixes[commonPrefix] = true
					return nil
				}
			}

			var size int64

			// Check if data is inline or in content-addressable storage
			if len(metadata.Data) > 0 {
				// Data is inline
				size = int64(len(metadata.Data))
			} else if metadata.Digest != "" {
				// Data is in content-addressable storage
				objPath, err := s.objectPath(metadata.Digest)
				if err != nil {
					return fmt.Errorf("invalid digest for object %s: %v", objectKey, err)
				}
				dataInfo, err := os.Stat(objPath)
				if err != nil {
					return fmt.Errorf("failed to stat content-addressed object for %s: %v", objectKey, err)
				}
				size = dataInfo.Size()
			}
			// else: size is 0 (empty/zero-byte object, including folder objects)

			// Always use meta file's ModTime
			objects = append(objects, ObjectInfo{
				Key:            objectKey,
				Size:           size,
				ETag:           metadata.ETag,
				ChecksumSHA256: urlSafeToStdBase64(metadata.ETag),
				ModTime:        info.ModTime(),
				Metadata:       metadata.Metadata,
			})
		}

		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	// Sort objects by key
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	// Apply maxKeys limit
	if maxKeys > 0 && len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	// Convert common prefixes to sorted slice
	var prefixes []string
	for prefix := range commonPrefixes {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)

	return objects, prefixes, nil
}

// CopyObject copies an object from one location to another
func (s *Storage) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*ObjectInfo, error) {
	// Verify source bucket exists
	if !s.BucketExists(srcBucket) {
		return nil, ErrBucketNotFound
	}

	// Verify destination bucket exists
	if !s.BucketExists(dstBucket) {
		return nil, ErrBucketNotFound
	}

	// Get source object directory
	srcObjectDir, err := s.safePath(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}

	srcMetaPath := filepath.Join(srcObjectDir, metaFile)

	// Load source metadata
	srcMetadata, err := loadObjectMetadata(srcMetaPath)
	if err != nil {
		return nil, err
	}
	if srcMetadata == nil {
		return nil, ErrObjectNotFound
	}

	// Get destination object directory
	dstObjectDir, err := s.safePath(dstBucket, dstKey)
	if err != nil {
		return nil, err
	}

	// Create destination object directory
	if err := os.MkdirAll(dstObjectDir, 0755); err != nil {
		return nil, err
	}

	dstMetaPath := filepath.Join(dstObjectDir, metaFile)

	// Check if destination object already exists
	var existingDstMetadata *objectMetadata
	if _, err := os.Stat(dstMetaPath); err == nil {
		existingDstMetadata, err = loadObjectMetadata(dstMetaPath)
		if err != nil {
			// If metadata is corrupted, treat as if object doesn't exist and overwrite
			existingDstMetadata = nil
		}
	}

	// Check compatibility: if destination exists with same ETag as source, it's a duplicate
	// This is compatible and we can skip the copy operation
	if existingDstMetadata != nil && existingDstMetadata.ETag == srcMetadata.ETag {
		// Same content already at destination - compatible duplicate, skip copy
		// Get size from existing destination
		var size int64
		if len(existingDstMetadata.Data) > 0 {
			size = int64(len(existingDstMetadata.Data))
		} else if existingDstMetadata.Digest != "" {
			// Get size from content-addressed object
			objFile, err := s.getContentAddressedObject(existingDstMetadata.Digest)
			if err != nil {
				return nil, err
			}
			defer objFile.Close()
			fileInfo, err := objFile.Stat()
			if err != nil {
				return nil, err
			}
			size = fileInfo.Size()
		}
		// else: size is 0 (zero-byte object, including folder objects)

		// Always use meta file's ModTime
		metaFileInfo, err := os.Stat(dstMetaPath)
		if err != nil {
			return nil, err
		}

		return &ObjectInfo{
			Key:            dstKey,
			Size:           size,
			ETag:           srcMetadata.ETag,
			ChecksumSHA256: urlSafeToStdBase64(srcMetadata.ETag),
			ModTime:        metaFileInfo.ModTime(),
			Metadata:       existingDstMetadata.Metadata,
		}, nil
	}

	// Check if source data is inline
	if len(srcMetadata.Data) > 0 {
		// Data is inline - copy directly
		dstMetadata := &objectMetadata{
			ETag:     srcMetadata.ETag,
			Data:     make([]byte, len(srcMetadata.Data)),
			Metadata: srcMetadata.Metadata,
			IsDir:    strings.HasSuffix(dstKey, "/"),
		}
		copy(dstMetadata.Data, srcMetadata.Data)

		if err := saveObjectMetadata(dstMetaPath, dstMetadata); err != nil {
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest
		if existingDstMetadata != nil && existingDstMetadata.Digest != "" {
			s.decrementRefCount(existingDstMetadata.Digest)
		}

		// Always use meta file's ModTime
		metaFileInfo, err := os.Stat(dstMetaPath)
		if err != nil {
			return nil, err
		}

		return &ObjectInfo{
			Key:            dstKey,
			Size:           int64(len(srcMetadata.Data)),
			ETag:           srcMetadata.ETag,
			ChecksumSHA256: urlSafeToStdBase64(srcMetadata.ETag),
			ModTime:        metaFileInfo.ModTime(),
			Metadata:       srcMetadata.Metadata,
		}, nil
	}

	// Check if source data is in content-addressable storage
	if srcMetadata.Digest != "" {
		// Data is in .objects - increment refcount first, then copy the digest reference
		if err := s.incrementRefCount(srcMetadata.Digest); err != nil {
			return nil, err
		}

		dstMetadata := &objectMetadata{
			ETag:     srcMetadata.ETag,
			Digest:   srcMetadata.Digest,
			Metadata: srcMetadata.Metadata,
			IsDir:    strings.HasSuffix(dstKey, "/"),
		}

		if err := saveObjectMetadata(dstMetaPath, dstMetadata); err != nil {
			// Rollback refcount increment
			s.decrementRefCount(srcMetadata.Digest)
			return nil, err
		}

		// Decrement refcount for old destination if it had a digest
		if existingDstMetadata != nil && existingDstMetadata.Digest != "" {
			s.decrementRefCount(existingDstMetadata.Digest)
		}

		// Fallback to reading from content-addressed object
		objFile, err := s.getContentAddressedObject(srcMetadata.Digest)
		if err != nil {
			return nil, err
		}
		defer objFile.Close()

		fileInfo, err := objFile.Stat()
		if err != nil {
			return nil, err
		}

		// Always use meta file's ModTime
		metaFileInfo, err := os.Stat(dstMetaPath)
		if err != nil {
			return nil, err
		}

		return &ObjectInfo{
			Key:            dstKey,
			Size:           fileInfo.Size(),
			ETag:           srcMetadata.ETag,
			ChecksumSHA256: urlSafeToStdBase64(srcMetadata.ETag),
			ModTime:        metaFileInfo.ModTime(),
			Metadata:       srcMetadata.Metadata,
		}, nil
	}

	// Zero-byte object (no digest and no inline data)
	dstMetadata := &objectMetadata{
		ETag:     srcMetadata.ETag,
		Metadata: srcMetadata.Metadata,
		IsDir:    strings.HasSuffix(dstKey, "/"),
	}

	if err := saveObjectMetadata(dstMetaPath, dstMetadata); err != nil {
		return nil, err
	}

	// Decrement refcount for old destination if it had a digest
	if existingDstMetadata != nil && existingDstMetadata.Digest != "" {
		s.decrementRefCount(existingDstMetadata.Digest)
	}

	// Always use meta file's ModTime
	metaFileInfo, err := os.Stat(dstMetaPath)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:            dstKey,
		Size:           0,
		ETag:           srcMetadata.ETag,
		ChecksumSHA256: urlSafeToStdBase64(srcMetadata.ETag),
		ModTime:        metaFileInfo.ModTime(),
		Metadata:       srcMetadata.Metadata,
	}, nil
}

// RenameObject renames an object within the same bucket
func (s *Storage) RenameObject(bucket, srcKey, dstKey string) error {
	// Verify bucket exists
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	// Get source object directory
	srcObjectDir, err := s.safePath(bucket, srcKey)
	if err != nil {
		return err
	}

	// Check if source exists by checking for meta file
	srcMetaPath := filepath.Join(srcObjectDir, metaFile)
	if _, err := os.Stat(srcMetaPath); err != nil {
		if os.IsNotExist(err) {
			return ErrObjectNotFound
		}
		return err
	}

	// Get destination object directory
	dstObjectDir, err := s.safePath(bucket, dstKey)
	if err != nil {
		return err
	}

	// Check if destination already exists (compatibility check)
	dstMetaPath := filepath.Join(dstObjectDir, metaFile)
	if _, err := os.Stat(dstMetaPath); err == nil {
		// Destination exists - check if it's the same as source
		// Load both metadata to compare
		srcMetadata, srcErr := loadObjectMetadata(srcMetaPath)
		dstMetadata, dstErr := loadObjectMetadata(dstMetaPath)

		// If both metadata are readable and ETags match, content is the same
		if srcErr == nil && dstErr == nil && srcMetadata != nil && dstMetadata != nil && srcMetadata.ETag == dstMetadata.ETag {
			// Same content - just delete source (no-op rename optimization)
			// Get bucket path for cleanup
			bucketPath, err := s.safePath(bucket, "")
			if err != nil {
				return err
			}

			if err := os.RemoveAll(srcObjectDir); err != nil {
				return err
			}

			// Clean up empty parent directories from source
			parentDir := filepath.Dir(srcObjectDir)
			s.cleanupEmptyDirs(parentDir, bucketPath)

			return nil
		}

		// Different content or corrupted metadata - delete destination and proceed with rename (overwrite)
		if err := os.RemoveAll(dstObjectDir); err != nil {
			return err
		}
	}

	// Get bucket path for cleanup before renaming
	bucketPath, err := s.safePath(bucket, "")
	if err != nil {
		return err
	}

	// Store the parent directory of source before renaming
	srcParentDir := filepath.Dir(srcObjectDir)

	// Create parent directory for destination
	if err := os.MkdirAll(filepath.Dir(dstObjectDir), 0755); err != nil {
		return err
	}

	// Rename/move the object directory
	if err := os.Rename(srcObjectDir, dstObjectDir); err != nil {
		return err
	}

	// Clean up empty parent directories from source
	s.cleanupEmptyDirs(srcParentDir, bucketPath)

	return nil
}
