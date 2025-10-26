package storage

import (
	"crypto/md5"
	"encoding/hex"
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
	metadata := map[string]string{
		"Content-Type": contentType,
	}
	if err := s.saveMetadata(uploadMetaPath, metadata); err != nil {
		return "", err
	}

	return uploadID, nil
}

// UploadPart uploads a part of a multipart upload
func (s *Storage) UploadPart(bucket, key, uploadID string, partNumber int, data io.Reader) (string, error) {
	if !s.BucketExists(bucket) {
		return "", ErrBucketNotFound
	}

	if partNumber < 1 || partNumber > 10000 {
		return "", ErrInvalidPartNumber
	}

	// Check filesystem for upload directory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return "", ErrInvalidUploadID
	}

	// Create temp file
	tmpFile, err := os.CreateTemp(uploadDir, ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate MD5 while writing
	hash := md5.New()
	writer := io.MultiWriter(tmpFile, hash)

	_, err = io.Copy(writer, data)
	if err != nil {
		tmpFile.Close()
		return "", err
	}
	tmpFile.Close()

	etag := hex.EncodeToString(hash.Sum(nil))

	partPath := filepath.Join(uploadDir, fmt.Sprintf("%d-%s", partNumber, etag))

	// Move temp file to part file
	if err := os.Rename(tmpFile.Name(), partPath); err != nil {
		return "", err
	}

	return etag, nil
}

// CompleteMultipartUpload completes a multipart upload
func (s *Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []Part) (string, error) {
	if !s.BucketExists(bucket) {
		return "", ErrBucketNotFound
	}

	// Check filesystem for upload directory if not in memory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return "", ErrInvalidUploadID
	}

	objectDir, err := s.safePath(bucket, key)
	if err != nil {
		return "", err
	}

	dataPath := filepath.Join(objectDir, dataFile)
	metaPath := filepath.Join(objectDir, metaFile)

	// Create object directory
	if err := os.MkdirAll(objectDir, 0755); err != nil {
		return "", err
	}

	// Create temp file for final object
	tmpFile, err := os.CreateTemp(objectDir, ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	hash := md5.New()

	// Concatenate parts in order
	for _, part := range parts {
		// Strip quotes from ETag if present (client may send quoted ETags)
		etag := strings.Trim(part.ETag, `"`)
		partPath := filepath.Join(uploadDir, fmt.Sprintf("%d-%s", part.PartNumber, etag))
		partFile, err := os.Open(partPath)
		if err != nil {
			tmpFile.Close()
			return "", err
		}

		if _, err := io.Copy(io.MultiWriter(tmpFile, hash), partFile); err != nil {
			partFile.Close()
			tmpFile.Close()
			return "", err
		}
		partFile.Close()
	}
	tmpFile.Close()

	// Move temp file to final location
	if err := os.Rename(tmpFile.Name(), dataPath); err != nil {
		return "", err
	}

	// Store metadata
	etag := hex.EncodeToString(hash.Sum(nil))

	uploadMetaPath := filepath.Join(uploadDir, metaFile)
	metadata, err := s.loadMetadata(uploadMetaPath)
	if err != nil {
		return "", err
	}

	metadata["ETag"] = etag
	if err := s.saveMetadata(metaPath, metadata); err != nil {
		return "", err
	}

	os.RemoveAll(uploadDir)

	return etag, nil
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

	if err := os.RemoveAll(uploadDir); err != nil {
		return err
	}

	return nil
}

// ListMultipartUploads lists all in-progress multipart uploads for a bucket
func (s *Storage) ListMultipartUploads(bucket, key string, maxUploads int) ([]MultipartUpload, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	// Check filesystem for upload directory
	uploadDir := filepath.Join(s.basePath, uploadsDir, bucket, key)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, nil
	}

	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		return nil, err
	}

	var uploads []MultipartUpload
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}

		uploadID := entry.Name()

		// Retrieve metadata or other information about the upload
		upload := MultipartUpload{
			UploadID: uploadID,
			Bucket:   bucket,
			Key:      key,
			Created:  info.ModTime(),
		}

		uploads = append(uploads, upload)
	}

	// Sort by creation time
	sort.Slice(uploads, func(i, j int) bool {
		return uploads[i].Created.Before(uploads[j].Created)
	})

	// Apply maxUploads limit
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}

	return uploads, nil
}

// ListParts lists all uploaded parts for a multipart upload
func (s *Storage) ListParts(bucket, key, uploadID string, maxParts int) ([]Part, error) {
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

		part := Part{
			PartNumber: partNumber,
			ETag:       etag,
			Size:       info.Size(),
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
