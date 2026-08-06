package server

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/wzshiming/s3d/pkg/storage"
)

const (
	// Default owner ID and display name for S3 objects
	defaultOwnerID          = "s3d-owner"
	defaultOwnerDisplayName = "s3d-owner"
)

// handlePutObject handles PutObject operation
func (s *S3Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Header.Get("x-amz-rename-source") != "" {
		s.handleRenameObject(w, r, bucket, key)
		return
	}
	if r.Header.Get("x-amz-copy-source") != "" {
		s.handleCopyObject(w, r, bucket, key)
		return
	}

	// Get the expected checksum from the request header (if provided)
	expectedChecksumSHA256 := r.Header.Get("x-amz-checksum-sha256")
	expectedChecksumMD5 := r.Header.Get("x-amz-checksum-md5")
	if expectedChecksumMD5 == "" {
		expectedChecksumMD5 = r.Header.Get("Content-MD5")
	}

	metadata := extractMetadata(r)

	objInfo, err := s.storage.PutObject(bucket, key, r.Body, metadata, expectedChecksumSHA256, expectedChecksumMD5)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", objInfo.ETag))
	if objInfo.ChecksumSHA256 != "" {
		w.Header().Set("x-amz-checksum-sha256", objInfo.ChecksumSHA256)
	}
	if objInfo.ChecksumMD5 != "" {
		w.Header().Set("x-amz-checksum-md5", objInfo.ChecksumMD5)
	}
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GetObject operation
func (s *S3Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	readerFunc, info, err := s.storage.GetObject(bucket, key)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", info.ETag))
	if info.ChecksumSHA256 != "" {
		w.Header().Set("x-amz-checksum-sha256", info.ChecksumSHA256)
	}
	if info.ChecksumMD5 != "" {
		w.Header().Set("x-amz-checksum-md5", info.ChecksumMD5)
	}
	setMetadataHeaders(w, info.Metadata)

	reader, err := readerFunc()
	if err != nil {
		s.response(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	http.ServeContent(w, r, key, info.ModTime, reader)
}

// handleDeleteObject handles DeleteObject operation
func (s *S3Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	err := s.storage.DeleteObject(bucket, key)
	if err != nil && err != storage.ErrObjectNotFound {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteObjects handles DeleteObjects operation (batch delete)
func (s *S3Handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	// Check if bucket exists
	if !s.storage.BucketExists(bucket) {
		s.response(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		return
	}

	// Parse the request body
	var deleteReq Delete
	if err := xml.NewDecoder(r.Body).Decode(&deleteReq); err != nil {
		s.response(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	// Process deletions
	result := DeleteObjectsResult{}

	for _, obj := range deleteReq.Objects {
		err := s.storage.DeleteObject(bucket, obj.Key)

		if err != nil && err != storage.ErrObjectNotFound {
			// Add to errors list
			result.Errors = append(result.Errors, DeleteError{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		} else {
			// Successfully deleted (or object didn't exist, which is also considered success in S3)
			if !deleteReq.Quiet {
				result.Deleted = append(result.Deleted, DeletedObject{
					Key: obj.Key,
				})
			}
		}
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleCopyObject handles CopyObject operation
func (s *S3Handler) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey string) {
	// Parse x-amz-copy-source header
	copySource := r.Header.Get("x-amz-copy-source")
	if copySource == "" {
		s.response(w, r, "InvalidArgument", "Copy source header is required", http.StatusBadRequest)
		return
	}

	// Remove leading slash if present
	copySource = strings.TrimPrefix(copySource, "/")

	// Parse source bucket and key
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		s.response(w, r, "InvalidArgument", "Invalid copy source format", http.StatusBadRequest)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Handle x-amz-metadata-directive header
	// COPY (default): copy metadata from source object
	// REPLACE: use metadata from request headers
	metadataDirective := r.Header.Get("x-amz-metadata-directive")
	storageClass := r.Header.Get("x-amz-storage-class")
	var metadata *storage.Metadata
	if metadataDirective == "REPLACE" {
		m := extractMetadata(r)
		metadata = &m
	} else if storageClass != "" {
		// Storage class applies to the copy even when metadata is copied
		// from the source object
		_, srcInfo, err := s.storage.GetObject(srcBucket, srcKey)
		if err != nil {
			s.errorResponse(w, r, err)
			return
		}
		m := srcInfo.Metadata
		m.StorageClass = storageClass
		metadata = &m
	}

	// Perform copy
	objInfo, err := s.storage.CopyObject(srcBucket, srcKey, dstBucket, dstKey, metadata)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := CopyObjectResult{
		LastModified: ISOTime(objInfo.ModTime),
		ETag:         fmt.Sprintf("%q", objInfo.ETag),
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleRenameObject handles RenameObject operation
func (s *S3Handler) handleRenameObject(w http.ResponseWriter, r *http.Request, bucket, dstKey string) {
	// Parse x-amz-rename-source header
	renameSource := r.Header.Get("x-amz-rename-source")
	if renameSource == "" {
		s.response(w, r, "InvalidArgument", "Rename source header is required", http.StatusBadRequest)
		return
	}

	// Remove leading slash if present
	renameSource = strings.TrimPrefix(renameSource, "/")

	// Parse source bucket and key
	parts := strings.SplitN(renameSource, "/", 2)
	if len(parts) != 2 {
		s.response(w, r, "InvalidArgument", "Invalid rename source format", http.StatusBadRequest)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Verify both source and destination are in the same bucket
	if srcBucket != bucket {
		s.response(w, r, "InvalidArgument", "RenameObject requires source and destination to be in the same bucket", http.StatusBadRequest)
		return
	}

	// Perform rename
	err := s.storage.RenameObject(bucket, srcKey, dstKey)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	// RenameObject returns 204 No Content on success
	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleListObjects handles ListObjects operation (v1 and v2)
func (s *S3Handler) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()

	// Check if this is ListObjectsV2 (list-type=2)
	if query.Get("list-type") == "2" {
		s.handleListObjectsV2(w, r, bucket)
		return
	}

	// ListObjects v1
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	marker := query.Get("marker")
	encodingType := query.Get("encoding-type")
	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		parsed, err := strconv.Atoi(mk)
		if err != nil || parsed < 0 {
			s.response(w, r, "InvalidArgument", "Argument max-keys must be an integer between 0 and 2147483647", http.StatusBadRequest)
			return
		}
		maxKeys = parsed
	}

	if maxKeys == 0 {
		result := ListBucketResult{
			Name:         bucket,
			Prefix:       prefix,
			Marker:       marker,
			Delimiter:    delimiter,
			EncodingType: encodingType,
			MaxKeys:      maxKeys,
		}

		s.xmlResponse(w, r, result, http.StatusOK)
		return
	}

	objects, commonPrefixes, nextMarker, err := s.storage.ListObjects(bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := ListBucketResult{
		Name:         bucket,
		Prefix:       prefix,
		Marker:       marker,
		Delimiter:    delimiter,
		EncodingType: encodingType,
		MaxKeys:      maxKeys,
		IsTruncated:  nextMarker != "",
		NextMarker:   nextMarker,
	}

	for _, obj := range objects {
		result.Contents = append(result.Contents, Contents{
			Key:          encodeKey(obj.Key, encodingType),
			LastModified: ISOTime(obj.ModTime),
			ETag:         fmt.Sprintf("%q", obj.ETag),
			Size:         obj.Size,
			StorageClass: storageClassOrDefault(obj.Metadata.StorageClass),
		})
	}

	for _, cp := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{
			Prefix: encodeKey(cp, encodingType),
		})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleListObjectsV2 handles ListObjectsV2 operation
func (s *S3Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	startAfter := query.Get("start-after")
	continuationToken := query.Get("continuation-token")
	encodingType := query.Get("encoding-type")
	fetchOwner := query.Get("fetch-owner") == "true"
	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		parsed, err := strconv.Atoi(mk)
		if err != nil || parsed < 0 {
			s.response(w, r, "InvalidArgument", "Argument max-keys must be an integer between 0 and 2147483647", http.StatusBadRequest)
			return
		}
		maxKeys = parsed
	}
	if maxKeys == 0 {
		result := ListBucketResultV2{
			Name:         bucket,
			Prefix:       prefix,
			Delimiter:    delimiter,
			EncodingType: encodingType,
			StartAfter:   startAfter,
			MaxKeys:      maxKeys,
		}

		s.xmlResponse(w, r, result, http.StatusOK)
		return
	}
	// Determine the marker to use
	marker := ""
	if continuationToken != "" {
		marker = continuationToken
	} else if startAfter != "" {
		marker = startAfter
	}

	objects, commonPrefixes, nextContinuationToken, err := s.storage.ListObjects(bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := ListBucketResultV2{
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		EncodingType:          encodingType,
		MaxKeys:               maxKeys,
		KeyCount:              len(objects) + len(commonPrefixes),
		IsTruncated:           nextContinuationToken != "",
		StartAfter:            startAfter,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextContinuationToken,
	}

	for _, obj := range objects {
		content := Contents{
			Key:          encodeKey(obj.Key, encodingType),
			LastModified: ISOTime(obj.ModTime),
			ETag:         fmt.Sprintf("%q", obj.ETag),
			Size:         obj.Size,
			StorageClass: storageClassOrDefault(obj.Metadata.StorageClass),
		}
		if fetchOwner {
			content.Owner = &Owner{
				ID:          defaultOwnerID,
				DisplayName: defaultOwnerDisplayName,
			}
		}
		result.Contents = append(result.Contents, content)
	}

	for _, cp := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{
			Prefix: encodeKey(cp, encodingType),
		})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// storageClassOrDefault returns the storage class or "STANDARD" if unset
func storageClassOrDefault(storageClass string) string {
	if storageClass == "" {
		return "STANDARD"
	}
	return storageClass
}

// encodeKey URL-encodes a value if encodingType is "url"
func encodeKey(value, encodingType string) string {
	if encodingType == "url" {
		return url.QueryEscape(value)
	}
	return value
}
