package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// genUploadID generates a unique upload ID using UUID
func genUploadID() string {
	return uuid.New().String()
}

// InitiateMultipartUpload initiates a multipart upload
func (s *Storage) InitiateMultipartUpload(bucket, key string, contentType string) (string, error) {
	if !s.BucketExists(bucket) {
		return "", ErrBucketNotFound
	}

	// Validate paths
	if err := sanitizeBucketName(bucket); err != nil {
		return "", err
	}
	if err := sanitizeObjectKey(key); err != nil {
		return "", err
	}

	// Generate upload ID
	uploadID := genUploadID()

	// Create upload directory in .uploads/bucket/key/uploadID
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return "", err
	}

	uploadMetaPath := filepath.Join(uploadDir, metaFile)
	metadata := &uploadMetadata{
		ContentType: contentType,
	}
	if err := saveUploadMetadata(uploadMetaPath, metadata); err != nil {
		return "", err
	}

	return uploadID, nil
}

// UploadPart uploads a part of a multipart upload
func (s *Storage) UploadPart(bucket, key, uploadID string, partNumber int, data io.Reader) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	if partNumber < 1 || partNumber > 10000 {
		return nil, ErrInvalidPartNumber
	}

	// Check filesystem for upload directory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	// Create temp file
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate SHA256 while writing
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	_, err = io.Copy(writer, data)
	if err != nil {
		tmpFile.Close()
		return nil, err
	}
	tmpFile.Close()

	etag := base64.URLEncoding.EncodeToString(hash.Sum(nil))

	partPath := filepath.Join(uploadDir, fmt.Sprintf("%d-%s", partNumber, etag))

	// Move temp file to part file
	if err := os.Rename(tmpFile.Name(), partPath); err != nil {
		return nil, err
	}

	// Get file info for size and mod time
	partFileInfo, err := os.Stat(partPath)
	if err != nil {
		return nil, err
	}

	// Load upload metadata for content type
	uploadMetaPath := filepath.Join(uploadDir, metaFile)
	metadata, _ := loadUploadMetadata(uploadMetaPath)
	contentType := "application/octet-stream"
	if metadata != nil && metadata.ContentType != "" {
		contentType = metadata.ContentType
	}

	return &ObjectInfo{
		Key:         key,
		Size:        partFileInfo.Size(),
		ETag:        etag,
		ModTime:     partFileInfo.ModTime(),
		ContentType: contentType,
	}, nil
}

// UploadPartCopy uploads a part of a multipart upload by copying from an existing object
func (s *Storage) UploadPartCopy(bucket, key, uploadID string, partNumber int, srcBucket, srcKey string) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	if partNumber < 1 || partNumber > 10000 {
		return nil, ErrInvalidPartNumber
	}

	// Check filesystem for upload directory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	// Verify source bucket exists
	if !s.BucketExists(srcBucket) {
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

	// Create temp file
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate SHA256 while copying
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	// Copy data from source (either inline or from data file)
	if len(srcMetadata.Data) > 0 {
		// Data is inline
		_, err = writer.Write(srcMetadata.Data)
	} else {
		// Data is in separate file
		srcDataPath := filepath.Join(srcObjectDir, dataFile)
		srcFile, err := os.Open(srcDataPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrObjectNotFound
			}
			return nil, err
		}
		defer srcFile.Close()
		_, err = io.Copy(writer, srcFile)
	}

	if err != nil {
		tmpFile.Close()
		return nil, err
	}
	tmpFile.Close()

	etag := base64.URLEncoding.EncodeToString(hash.Sum(nil))

	partPath := filepath.Join(uploadDir, fmt.Sprintf("%d-%s", partNumber, etag))

	// Move temp file to part file
	if err := os.Rename(tmpFile.Name(), partPath); err != nil {
		return nil, err
	}

	// Get file info for size and mod time
	partFileInfo, err := os.Stat(partPath)
	if err != nil {
		return nil, err
	}

	// Load upload metadata for content type
	uploadMetaPath := filepath.Join(uploadDir, metaFile)
	metadata, _ := loadUploadMetadata(uploadMetaPath)
	contentType := "application/octet-stream"
	if metadata != nil && metadata.ContentType != "" {
		contentType = metadata.ContentType
	}

	return &ObjectInfo{
		Key:         key,
		Size:        partFileInfo.Size(),
		ETag:        etag,
		ModTime:     partFileInfo.ModTime(),
		ContentType: contentType,
	}, nil
}

// CompleteMultipartUpload completes a multipart upload
func (s *Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []Multipart) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	// Check filesystem for upload directory if not in memory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	objectDir, err := s.safePath(bucket, key)
	if err != nil {
		return nil, err
	}

	dataPath := filepath.Join(objectDir, dataFile)
	metaPath := filepath.Join(objectDir, metaFile)

	// Create object directory
	if err := os.MkdirAll(objectDir, 0755); err != nil {
		return nil, err
	}

	// Create temp file for final object
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Concatenate parts in order using sendfile for better performance
	for _, part := range parts {
		// Strip quotes from ETag if present (client may send quoted ETags)
		etag := strings.Trim(part.ETag, `"`)
		partPath := filepath.Join(uploadDir, fmt.Sprintf("%d-%s", part.PartNumber, etag))
		partFile, err := os.Open(partPath)
		if err != nil {
			tmpFile.Close()
			return nil, err
		}

		if _, err := copyFileWithSendfile(tmpFile, partFile); err != nil {
			partFile.Close()
			tmpFile.Close()
			return nil, err
		}
		partFile.Close()
	}
	tmpFile.Close()

	// Move temp file to final location
	if err := os.Rename(tmpFile.Name(), dataPath); err != nil {
		return nil, err
	}

	// Calculate hash from the final file
	// Note: This requires an additional read pass, but it's necessary because:
	// 1. sendfile is a zero-copy operation that transfers data in kernel space
	// 2. To hash the data, we must read it into user space
	// 3. Using a tee reader would defeat the purpose by forcing data through user space during concatenation
	// The overall performance is still better: sendfile (zero-copy merge) + one read (for hash)
	// vs. the original approach: multiple reads through io.MultiWriter (copy + hash simultaneously)
	hash := sha256.New()
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	
	if _, err := io.Copy(hash, dataFile); err != nil {
		return nil, err
	}

	// Store metadata - use URL-safe base64 encoded SHA256
	etag := base64.URLEncoding.EncodeToString(hash.Sum(nil))

	uploadMetaPath := filepath.Join(uploadDir, metaFile)
	uploadMetadata, err := loadUploadMetadata(uploadMetaPath)
	if err != nil {
		return nil, err
	}

	// Create object metadata from upload metadata
	objectMetadata := &objectMetadata{
		ContentType: uploadMetadata.ContentType,
		ETag:        etag,
	}
	if err := saveObjectMetadata(metaPath, objectMetadata); err != nil {
		return nil, err
	}

	// Get size from data file
	dataFileInfo, err := os.Stat(dataPath)
	if err != nil {
		return nil, err
	}

	// Always use meta file's ModTime
	metaFileInfo, err := os.Stat(metaPath)
	if err != nil {
		return nil, err
	}

	contentType := objectMetadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Get the uploads base directory as the stop point
	uploadsBaseDir := filepath.Join(s.basePath, uploadsDir)

	// Store the parent directory before deletion
	parentDir := filepath.Dir(uploadDir)

	if err := os.RemoveAll(uploadDir); err != nil {
		return nil, err
	}

	// Clean up empty parent directories
	s.cleanupEmptyDirs(parentDir, uploadsBaseDir)

	return &ObjectInfo{
		Key:         key,
		Size:        dataFileInfo.Size(),
		ETag:        etag,
		ModTime:     metaFileInfo.ModTime(),
		ContentType: contentType,
	}, nil
}

// AbortMultipartUpload aborts a multipart upload
func (s *Storage) AbortMultipartUpload(bucket, key, uploadID string) error {
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	// Check filesystem for upload directory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return ErrInvalidUploadID
	}

	// Get the uploads base directory as the stop point
	uploadsBaseDir := filepath.Join(s.basePath, uploadsDir)

	// Store the parent directory before deletion
	parentDir := filepath.Dir(uploadDir)

	if err := os.RemoveAll(uploadDir); err != nil {
		return err
	}

	// Clean up empty parent directories
	s.cleanupEmptyDirs(parentDir, uploadsBaseDir)

	return nil
}

// ListMultipartUploads lists all in-progress multipart uploads for a bucket
// ListMultipartUploads lists multipart uploads with pagination support
func (s *Storage) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]MultipartUpload, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	// Check filesystem for upload directory
	uploadBaseDir := filepath.Join(s.basePath, uploadsDir, bucket)
	if _, err := os.Stat(uploadBaseDir); os.IsNotExist(err) {
		return nil, nil
	}

	var uploads []MultipartUpload

	// Walk through the uploads directory
	err := filepath.Walk(uploadBaseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if !info.IsDir() {
			return nil
		}

		// Get relative path from uploadBaseDir
		relPath, err := filepath.Rel(uploadBaseDir, path)
		if err != nil || relPath == "." {
			return nil
		}

		// Check if this directory contains a meta file (indicating it's an upload directory)
		metaPath := filepath.Join(path, metaFile)
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			return nil // Not an upload directory, keep walking
		}

		// This is an upload directory: .uploads/bucket/key/uploadID
		// Split the relative path to get key and uploadID
		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) < 2 {
			return nil
		}

		uploadID := parts[len(parts)-1]
		key := strings.Join(parts[:len(parts)-1], "/")

		// Apply prefix filter
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		// Apply marker filter
		if keyMarker != "" {
			if key < keyMarker {
				return nil
			}
			if key == keyMarker && uploadIDMarker != "" && uploadID <= uploadIDMarker {
				return nil
			}
		}

		upload := MultipartUpload{
			UploadID: uploadID,
			Bucket:   bucket,
			Key:      key,
			ModTime:  info.ModTime(),
		}

		uploads = append(uploads, upload)
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort by key, then by upload ID
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})

	// Apply maxUploads limit
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}

	return uploads, nil
}

// ListParts lists all uploaded parts for a multipart upload with pagination support
func (s *Storage) ListParts(bucket, key, uploadID string, partNumberMarker, maxParts int) ([]Part, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	// Check filesystem for upload directory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		return nil, err
	}

	var parts []Part
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		info, err := entry.Info()
		if err != nil {
			continue
		}

		var partNumber int
		var etag string
		n, err := fmt.Sscanf(name, "%d-%s", &partNumber, &etag)
		if err != nil || n != 2 {
			continue
		}

		// Apply marker filter
		if partNumberMarker > 0 && partNumber <= partNumberMarker {
			continue
		}

		part := Part{
			PartNumber: partNumber,
			ETag:       etag,
			Size:       info.Size(),
			ModTime:    info.ModTime(),
		}

		parts = append(parts, part)
	}

	// Sort by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Apply maxParts limit
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
	}

	return parts, nil
}
