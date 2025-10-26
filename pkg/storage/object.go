package storage

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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

	// Create temp file in the object directory
	tmpFile, err := os.CreateTemp(objectDir, ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate MD5 while writing
	hash := md5.New()
	writer := io.MultiWriter(tmpFile, hash)

	if _, err := io.Copy(writer, data); err != nil {
		tmpFile.Close()
		return "", err
	}
	tmpFile.Close()

	// Move temp file to data file
	if err := os.Rename(tmpFile.Name(), dataPath); err != nil {
		return "", err
	}

	// Store metadata
	etag := hex.EncodeToString(hash.Sum(nil))
	metadata := &Metadata{
		ContentType: contentType,
		ETag:        etag,
	}
	if err := s.saveMetadata(metaPath, metadata); err != nil {
		return "", err
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

	// Check if object exists
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

	// Load metadata
	metadata, err := s.loadMetadata(metaPath)
	if err != nil {
		file.Close()
		return nil, nil, err
	}

	info := &ObjectInfo{
		Key:          key,
		Size:         fileInfo.Size(),
		ETag:         metadata.ETag,
		LastModified: fileInfo.ModTime(),
		ContentType:  metadata.ContentType,
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

	// Check if object exists
	if _, err := os.Stat(filepath.Join(objectDir, dataFile)); os.IsNotExist(err) {
		return ErrObjectNotFound
	}

	// Remove entire object directory
	return os.RemoveAll(objectDir)
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

		// Check if this is a data file
		if filepath.Base(path) == dataFile && !info.IsDir() {
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
			metaPath := filepath.Join(objectDir, metaFile)
			metadata, _ := s.loadMetadata(metaPath)

			objects = append(objects, ObjectInfo{
				Key:          objectKey,
				Size:         info.Size(),
				ETag:         metadata.ETag,
				LastModified: info.ModTime(),
				ContentType:  metadata.ContentType,
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

	srcDataPath := filepath.Join(srcObjectDir, dataFile)
	srcMetaPath := filepath.Join(srcObjectDir, metaFile)

	// Check if source exists
	srcFile, err := os.Open(srcDataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrObjectNotFound
		}
		return "", err
	}
	defer srcFile.Close()

	// Get source metadata
	srcMetadata, _ := s.loadMetadata(srcMetaPath)

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

	dstDataPath := filepath.Join(dstObjectDir, dataFile)
	dstMetaPath := filepath.Join(dstObjectDir, metaFile)

	// Create temp file for destination
	tmpFile, err := os.CreateTemp(dstObjectDir, ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	// Copy data and calculate MD5
	hash := md5.New()
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

	// Store metadata
	etag := hex.EncodeToString(hash.Sum(nil))
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

	srcDataPath := filepath.Join(srcObjectDir, dataFile)

	// Check if source exists
	if _, err := os.Stat(srcDataPath); err != nil {
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

	// Create parent directory for destination
	if err := os.MkdirAll(filepath.Dir(dstObjectDir), 0755); err != nil {
		return err
	}

	// Rename/move the object directory
	if err := os.Rename(srcObjectDir, dstObjectDir); err != nil {
		return err
	}

	return nil
}
