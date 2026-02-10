package storage

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func (s *Storage) uploadPath(bucket, key, uploadID string) string {
	return filepath.Join(s.basePath, uploadsDir, bucket, key, uploadID)
}

func (s *Storage) getUploadMetadata(bucket, key, uploadID string) (*uploadMetadata, error) {
	var metadata uploadMetadata
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(uploadsBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		val := b.Get([]byte(uploadID))
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

	if metadata.Key != key {
		return nil, ErrUploadNotFound
	}
	return &metadata, nil
}

func (s *Storage) putUploadMetadata(bucket, key string, metadata *uploadMetadata) (uploadID string, err error) {
	var buf bytes.Buffer
	metadata.Key = key
	encoder := gob.NewEncoder(&buf)
	err = encoder.Encode(metadata)
	if err != nil {
		return "", err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(uploadsBucketPrefix + bucket))
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		uploadID = fmt.Sprintf("s-%d", id)
		return b.Put([]byte(uploadID), buf.Bytes())
	})
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

func (s *Storage) deleteUploadMetadata(bucket, key, uploadID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(uploadsBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		return b.Delete([]byte(uploadID))
	})
}

// InitiateMultipartUpload initiates a multipart upload
func (s *Storage) InitiateMultipartUpload(bucket, key string, userMetadata Metadata) (string, error) {
	if !s.BucketExists(bucket) {
		return "", ErrBucketNotFound
	}

	metadata := &uploadMetadata{
		Metadata: userMetadata,
		ModTime:  time.Now(),
	}

	// Store upload metadata in BoltDB for listing
	uploadID, err := s.putUploadMetadata(bucket, key, metadata)
	if err != nil {
		return "", err
	}

	// Create upload directory in .uploads/bucket/key/uploadID
	uploadDir := filepath.Join(s.uploadsDir, bucket, key, uploadID)
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return "", err
	}

	return uploadID, nil
}

// UploadPart uploads a part of a multipart upload
// If expectedChecksumSHA256 is provided (non-empty), it validates the checksum after computing.
// If expectedChecksumMD5 is provided (non-empty), it validates the MD5 checksum after computing.
func (s *Storage) UploadPart(bucket, key, uploadID string, partNumber int, data io.Reader, expectedChecksumSHA256 string, expectedChecksumMD5 string) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	if partNumber < 1 || partNumber > 10000 {
		return nil, ErrInvalidPartNumber
	}

	// Check filesystem for upload directory
	uploadDir := s.uploadPath(bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	// Create temp file
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate SHA256 and MD5 while writing
	hashSHA256 := sha256.New()
	hashMD5 := md5.New()
	writer := io.MultiWriter(tmpFile, hashSHA256, hashMD5)

	_, err = io.Copy(writer, data)
	if err != nil {
		tmpFile.Close()
		return nil, err
	}
	tmpFile.Close()

	sumSHA256 := hashSHA256.Sum(nil)
	checksumSHA256 := base64.StdEncoding.EncodeToString(sumSHA256)

	sumMD5 := hashMD5.Sum(nil)
	etag := hex.EncodeToString(sumMD5)
	checksumMD5 := base64.StdEncoding.EncodeToString(sumMD5)

	// Validate checksums if provided
	if expectedChecksumSHA256 != "" && expectedChecksumSHA256 != checksumSHA256 {
		return nil, ErrChecksumMismatch
	}
	if expectedChecksumMD5 != "" && expectedChecksumMD5 != checksumMD5 {
		return nil, ErrChecksumMismatch
	}

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

	metadata, err := s.getUploadMetadata(bucket, key, uploadID)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:            key,
		Size:           partFileInfo.Size(),
		ETag:           etag,
		ChecksumSHA256: checksumSHA256,
		ChecksumMD5:    checksumMD5,
		ModTime:        partFileInfo.ModTime(),
		Metadata:       metadata.Metadata,
	}, nil
}

// UploadPartCopy uploads a part of a multipart upload by copying from an existing object
// If startByte and endByte are both >= 0, only the specified byte range is copied.
// If startByte is < 0, the entire source object is copied.
func (s *Storage) UploadPartCopy(bucket, key, uploadID string, partNumber int, srcBucket, srcKey string, startByte, endByte int64) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	if partNumber < 1 || partNumber > 10000 {
		return nil, ErrInvalidPartNumber
	}

	// Check filesystem for upload directory
	uploadDir := s.uploadPath(bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	// Verify source bucket exists
	if !s.BucketExists(srcBucket) {
		return nil, ErrBucketNotFound
	}

	// Get source object metadata to determine size and validate byte range
	srcMetadata, err := s.getObjectMetadata(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	if srcMetadata == nil {
		return nil, ErrObjectNotFound
	}

	// Validate byte range if specified
	hasRange := startByte >= 0
	if hasRange {
		if startByte > endByte || startByte >= srcMetadata.Size || endByte >= srcMetadata.Size {
			return nil, ErrInvalidRange
		}
	}

	// Create temp file
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Calculate SHA256 and MD5 while copying
	hashSHA256 := sha256.New()
	hashMD5 := md5.New()
	writer := io.MultiWriter(tmpFile, hashSHA256, hashMD5)

	// Copy data from source (either inline or digest)
	if len(srcMetadata.Data) > 0 {
		// Data is inline
		if hasRange {
			_, err = writer.Write(srcMetadata.Data[startByte : endByte+1])
		} else {
			_, err = writer.Write(srcMetadata.Data)
		}
	} else if srcMetadata.Etag != "" {
		// Data is in content-addressable storage
		srcFile, err := s.getContentAddressedObject(srcMetadata.Etag)
		if err != nil {
			return nil, err
		}
		defer srcFile.Close()

		if hasRange {
			// Seek to start position and copy only the range
			if _, err := srcFile.Seek(startByte, io.SeekStart); err != nil {
				return nil, err
			}
			_, err = io.CopyN(writer, srcFile, endByte-startByte+1)
		} else {
			_, err = io.Copy(writer, srcFile)
		}
	}

	if err != nil {
		tmpFile.Close()
		return nil, err
	}
	tmpFile.Close()

	sumSHA256 := hashSHA256.Sum(nil)
	checksumSHA256 := base64.StdEncoding.EncodeToString(sumSHA256)

	sumMD5 := hashMD5.Sum(nil)
	etag := hex.EncodeToString(sumMD5)
	checksumMD5 := base64.StdEncoding.EncodeToString(sumMD5)

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

	metadata, err := s.getUploadMetadata(bucket, key, uploadID)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:            key,
		Size:           partFileInfo.Size(),
		ETag:           etag,
		ChecksumSHA256: checksumSHA256,
		ChecksumMD5:    checksumMD5,
		ModTime:        partFileInfo.ModTime(),
		Metadata:       metadata.Metadata,
	}, nil
}

func normalEtag(etag string) string {
	return strings.Trim(etag, "\"")
}

// CompleteMultipartUpload completes a multipart upload
func (s *Storage) CompleteMultipartUpload(bucket, key, uploadID string, parts []Multipart, expectedChecksumSHA256 string, expectedChecksumMD5 string) (*ObjectInfo, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	// Check filesystem for upload directory if not in memory
	uploadDir := s.uploadPath(bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return nil, ErrInvalidUploadID
	}

	// Create temp file for the complete object
	tmpFile, err := s.tempFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())

	// Assemble parts into temp file and calculate SHA256 and MD5
	hashSHA256 := sha256.New()
	hashMD5 := md5.New()
	writer := io.MultiWriter(tmpFile, hashSHA256, hashMD5)

	for _, part := range parts {
		partPath := filepath.Join(uploadDir, fmt.Sprintf("%d-%s", part.PartNumber, normalEtag(part.ETag)))
		partFile, err := os.Open(partPath)
		if err != nil {
			tmpFile.Close()
			return nil, fmt.Errorf("failed to open part file: %v", err)
		}

		_, err = io.Copy(writer, partFile)
		partFile.Close()
		if err != nil {
			tmpFile.Close()
			return nil, err
		}
	}

	tmpFile.Close()

	// Check file size to determine if it should be inlined
	fileInfo, err := os.Stat(tmpFile.Name())
	if err != nil {
		return nil, err
	}

	sumSHA256 := hashSHA256.Sum(nil)
	checksumSHA256 := base64.StdEncoding.EncodeToString(sumSHA256)

	sumMD5 := hashMD5.Sum(nil)
	etag := hex.EncodeToString(sumMD5)
	checksumMD5 := base64.StdEncoding.EncodeToString(sumMD5)

	// Validate checksums if provided
	if expectedChecksumSHA256 != "" && expectedChecksumSHA256 != checksumSHA256 {
		return nil, ErrChecksumMismatch
	}
	if expectedChecksumMD5 != "" && expectedChecksumMD5 != checksumMD5 {
		return nil, ErrChecksumMismatch
	}

	// Move temp file to final object location
	err = s.storeContentAddressedObject(tmpFile.Name(), etag)
	if err != nil {
		return nil, err
	}

	// Load upload metadata for user metadata
	uploadMetadata, err := s.getUploadMetadata(bucket, key, uploadID)
	if err != nil {
		return nil, err
	}

	metadata := &objectMetadata{
		Size:     fileInfo.Size(),
		Etag:     etag,
		Sha256:   checksumSHA256,
		Md5:      checksumMD5,
		Metadata: uploadMetadata.Metadata,
		ModTime:  fileInfo.ModTime(),
	}

	if err := s.putObjectMetadata(bucket, key, metadata); err != nil {
		return nil, err
	}

	// Cleanup: delete upload directory and metadata
	if err := os.RemoveAll(uploadDir); err != nil {
		return nil, err
	}
	if err := s.deleteUploadMetadata(bucket, key, uploadID); err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:            key,
		Size:           metadata.Size,
		ETag:           metadata.Etag,
		ChecksumSHA256: metadata.Sha256,
		ChecksumMD5:    metadata.Md5,
		ModTime:        metadata.ModTime,
		Metadata:       metadata.Metadata,
	}, nil
}

// AbortMultipartUpload aborts a multipart upload
func (s *Storage) AbortMultipartUpload(bucket, key, uploadID string) error {
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	// Check filesystem for upload directory
	uploadDir := s.uploadPath(bucket, key, uploadID)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		return ErrInvalidUploadID
	}

	if err := os.RemoveAll(uploadDir); err != nil {
		return err
	}
	if err := s.deleteUploadMetadata(bucket, key, uploadID); err != nil {
		return err
	}

	return nil
}

func (s *Storage) walkUploads(bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int, walkFunc func(key, uploadID string, metadata *uploadMetadata) error) (nextKeyMarker, nextUploadIDMarker string, err error) {
	var handledKeys int
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(uploadsBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			uploadID := string(k)
			var metadata uploadMetadata
			decoder := gob.NewDecoder(bytes.NewReader(v))
			if err := decoder.Decode(&metadata); err != nil {
				continue
			}

			key := metadata.Key

			if prefix != "" && !bytes.HasPrefix([]byte(key), []byte(prefix)) {
				continue
			}
			if keyMarker != "" && key <= keyMarker {
				continue
			}
			if uploadIDMarker != "" && key == keyMarker && uploadID <= uploadIDMarker {
				continue
			}

			if err := walkFunc(key, uploadID, &metadata); err != nil {
				return err
			}

			handledKeys++
			if handledKeys >= maxUploads {
				nextKeyMarker = key
				nextUploadIDMarker = uploadID
				break
			}
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return nextKeyMarker, nextUploadIDMarker, nil
}

// ListMultipartUploads lists all in-progress multipart uploads with pagination support
func (s *Storage) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (uploads []MultipartUpload, nextKeyMarker, nextUploadIDMarker string, err error) {
	if !s.BucketExists(bucket) {
		return nil, "", "", ErrBucketNotFound
	}

	nextKeyMarker, nextUploadIDMarker, err = s.walkUploads(bucket, prefix, keyMarker, uploadIDMarker, maxUploads, func(key, uploadID string, metadata *uploadMetadata) error {
		uploads = append(uploads, MultipartUpload{
			UploadID: uploadID,
			Bucket:   bucket,
			Key:      key,
			ModTime:  metadata.ModTime,
		})
		return nil
	})
	if err != nil {
		return nil, "", "", err
	}

	return uploads, nextKeyMarker, nextUploadIDMarker, nil
}

func (s *Storage) walkParts(bucket, key, uploadID string, partNumberMarker string, maxParts int, walkFn func(partNumber int, metadata *objectMetadata) error) (nextPartNumberMarker string, err error) {
	uploadDir := s.uploadPath(bucket, key, uploadID)
	files, err := os.ReadDir(uploadDir)
	if err != nil {
		return "", err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	var handledParts int

	partNumberMarkerInt := 0
	if partNumberMarker != "" {
		partNumberMarkerInt, _ = strconv.Atoi(partNumberMarker)
	}

	if partNumberMarkerInt > 0 && partNumberMarkerInt > len(files) {
		return "", nil
	}

	files = files[partNumberMarkerInt:]

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		var partNumber int
		var etag string
		n, err := fmt.Sscanf(name, "%d-%s", &partNumber, &etag)
		if err != nil || n != 2 {
			continue
		}

		partPath := filepath.Join(uploadDir, name)
		partFileInfo, err := os.Stat(partPath)
		if err != nil {
			return "", err
		}

		metadata := &objectMetadata{
			Size:     partFileInfo.Size(),
			Etag:     etag,
			ModTime:  partFileInfo.ModTime(),
			Metadata: Metadata{},
		}

		if err := walkFn(partNumber, metadata); err != nil {
			return "", err
		}

		handledParts++
		if handledParts >= maxParts {
			nextPartNumberMarker = strconv.Itoa(partNumber)
			break
		}
	}

	return nextPartNumberMarker, nil
}

// ListParts lists all uploaded parts for a multipart upload with pagination support
func (s *Storage) ListParts(bucket, key, uploadID string, partNumberMarker string, maxParts int) (parts []Part, nextPartNumberMarker string, err error) {
	if !s.BucketExists(bucket) {
		return nil, "", ErrBucketNotFound
	}

	nextPartNumberMarker, err = s.walkParts(bucket, key, uploadID, partNumberMarker, maxParts, func(partNumber int, metadata *objectMetadata) error {
		parts = append(parts, Part{
			PartNumber: partNumber,
			ETag:       metadata.Etag,
			Size:       metadata.Size,
			ModTime:    metadata.ModTime,
		})
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return parts, nextPartNumberMarker, nil
}
