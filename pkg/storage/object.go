package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// inlineDataReader wraps a bytes.Reader to implement io.ReadSeekCloser
type inlineDataReader struct {
	*bytes.Reader
}

// Close implements io.Closer (no-op for in-memory data)
func (r *inlineDataReader) Close() error {
	return nil
}

// PutObject stores an object
func (s *Storage) PutObject(bucket, key string, data io.Reader, contentType string) (string, error) {
	if !s.BucketExists(bucket) {
		return "", ErrBucketNotFound
	}

	objectDir, err := s.safePath(bucket, key)
	if err != nil {
		return "", err
	}

	// Create object directory
	if err := os.MkdirAll(objectDir, 0755); err != nil {
		return "", err
	}

	dataPath := filepath.Join(objectDir, dataFile)
	metaPath := filepath.Join(objectDir, metaFile)

	// Check if object already exists and load existing metadata
	var existingMetadata *Metadata
	if _, err := os.Stat(metaPath); err == nil {
		existingMetadata, err = s.loadMetadata(metaPath)
		if err != nil {
			// If metadata is corrupted, treat as if object doesn't exist and overwrite
			existingMetadata = nil
		}
	}

	// Create temp file in the object directory
	tmpFile, err := s.tempFile()
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate SHA256 while writing
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	if _, err := io.Copy(writer, data); err != nil {
		tmpFile.Close()
		return "", err
	}
	tmpFile.Close()

	// Check file size to determine if it should be inlined
	fileInfo, err := os.Stat(tmpFile.Name())
	if err != nil {
		return "", err
	}

	etag := base64.URLEncoding.EncodeToString(hash.Sum(nil))

	// Check compatibility: if object exists with same ETag, it's a duplicate write
	// This is compatible and we can proceed without issue (S3 behavior)
	if existingMetadata != nil && existingMetadata.ETag == etag {
		// Same content - compatible duplicate write, just return existing ETag
		// No need to rewrite the object
		// Note: tmpFile is already cleaned up by defer
		return etag, nil
	}

	metadata := &Metadata{
		ContentType: contentType,
		ETag:        etag,
	}

	// If file is small enough, embed it in metadata
	if fileInfo.Size() <= inlineThreshold {
		// Read the file content
		fileData, err := os.ReadFile(tmpFile.Name())
		if err != nil {
			return "", err
		}
		metadata.Data = fileData

		// Save metadata with inline data
		if err := s.saveMetadata(metaPath, metadata); err != nil {
			return "", err
		}
		// No need to create a separate data file
		// If there was an old data file (object was previously large), clean it up
		if existingMetadata != nil && len(existingMetadata.Data) == 0 {
			os.Remove(dataPath)
		}
	} else {
		// Move temp file to data file for larger files
		if err := os.Rename(tmpFile.Name(), dataPath); err != nil {
			return "", err
		}

		// Store metadata without inline data
		if err := s.saveMetadata(metaPath, metadata); err != nil {
			return "", err
		}
	}

	return etag, nil
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

	dataPath := filepath.Join(objectDir, dataFile)
	metaPath := filepath.Join(objectDir, metaFile)

	// Load metadata first
	metadata, err := s.loadMetadata(metaPath)
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
			Key:         key,
			Size:        int64(len(metadata.Data)),
			ETag:        metadata.ETag,
			ModTime:     metaFileInfo.ModTime(),
			ContentType: metadata.ContentType,
		}

		if info.ContentType == "" {
			info.ContentType = "application/octet-stream"
		}

		return reader, info, nil
	}

	// Data is in separate file
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}

	info := &ObjectInfo{
		Key:         key,
		Size:        fileInfo.Size(),
		ETag:        metadata.ETag,
		ModTime:     fileInfo.ModTime(),
		ContentType: metadata.ContentType,
	}

	if info.ContentType == "" {
		info.ContentType = "application/octet-stream"
	}

	return file, info, nil
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

			// Load metadata
			metadata, _ := s.loadMetadata(path)

			var size int64
			var modTime time.Time

			// Check if data is inline or in separate file
			if metadata != nil && len(metadata.Data) > 0 {
				// Data is inline
				size = int64(len(metadata.Data))
				modTime = info.ModTime()
			} else {
				// Data is in separate file
				dataPath := filepath.Join(objectDir, dataFile)
				dataInfo, err := os.Stat(dataPath)
				if err != nil {
					return nil // Skip if data file doesn't exist
				}
				size = dataInfo.Size()
				modTime = dataInfo.ModTime()
			}

			objects = append(objects, ObjectInfo{
				Key:         objectKey,
				Size:        size,
				ETag:        metadata.ETag,
				ModTime:     modTime,
				ContentType: metadata.ContentType,
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
func (s *Storage) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (string, error) {
	// Verify source bucket exists
	if !s.BucketExists(srcBucket) {
		return "", ErrBucketNotFound
	}

	// Verify destination bucket exists
	if !s.BucketExists(dstBucket) {
		return "", ErrBucketNotFound
	}

	// Get source object directory
	srcObjectDir, err := s.safePath(srcBucket, srcKey)
	if err != nil {
		return "", err
	}

	srcMetaPath := filepath.Join(srcObjectDir, metaFile)

	// Load source metadata
	srcMetadata, err := s.loadMetadata(srcMetaPath)
	if err != nil {
		return "", err
	}
	if srcMetadata == nil {
		return "", ErrObjectNotFound
	}

	contentType := srcMetadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Get destination object directory
	dstObjectDir, err := s.safePath(dstBucket, dstKey)
	if err != nil {
		return "", err
	}

	// Create destination object directory
	if err := os.MkdirAll(dstObjectDir, 0755); err != nil {
		return "", err
	}

	dstMetaPath := filepath.Join(dstObjectDir, metaFile)
	dstDataPath := filepath.Join(dstObjectDir, dataFile)

	// Check if destination object already exists
	var existingDstMetadata *Metadata
	if _, err := os.Stat(dstMetaPath); err == nil {
		existingDstMetadata, err = s.loadMetadata(dstMetaPath)
		if err != nil {
			// If metadata is corrupted, treat as if object doesn't exist and overwrite
			existingDstMetadata = nil
		}
	}

	// Check compatibility: if destination exists with same ETag as source, it's a duplicate
	// This is compatible and we can skip the copy operation
	if existingDstMetadata != nil && existingDstMetadata.ETag == srcMetadata.ETag {
		// Same content already at destination - compatible duplicate, skip copy
		return srcMetadata.ETag, nil
	}

	// Check if source data is inline
	if len(srcMetadata.Data) > 0 {
		// Data is inline - copy directly
		dstMetadata := &Metadata{
			ContentType: contentType,
			ETag:        srcMetadata.ETag,
			Data:        make([]byte, len(srcMetadata.Data)),
		}
		copy(dstMetadata.Data, srcMetadata.Data)

		if err := s.saveMetadata(dstMetaPath, dstMetadata); err != nil {
			return "", err
		}

		// If destination previously had separate data file, clean it up
		if existingDstMetadata != nil && len(existingDstMetadata.Data) == 0 {
			os.Remove(dstDataPath)
		}

		return srcMetadata.ETag, nil
	}

	// Data is in separate file
	srcDataPath := filepath.Join(srcObjectDir, dataFile)
	srcFile, err := os.Open(srcDataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrObjectNotFound
		}
		return "", err
	}
	defer srcFile.Close()

	// Create temp file for destination
	tmpFile, err := s.tempFile()
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	// Copy data and calculate SHA256
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	if _, err := io.Copy(writer, srcFile); err != nil {
		tmpFile.Close()
		return "", err
	}
	tmpFile.Close()

	// Move temp file to final location
	if err := os.Rename(tmpFile.Name(), dstDataPath); err != nil {
		return "", err
	}

	// Store metadata - use URL-safe base64 encoded SHA256
	etag := base64.URLEncoding.EncodeToString(hash.Sum(nil))
	dstMetadata := &Metadata{
		ContentType: contentType,
		ETag:        etag,
	}
	if err := s.saveMetadata(dstMetaPath, dstMetadata); err != nil {
		return "", err
	}

	return etag, nil
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
		srcMetadata, srcErr := s.loadMetadata(srcMetaPath)
		dstMetadata, dstErr := s.loadMetadata(dstMetaPath)

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
