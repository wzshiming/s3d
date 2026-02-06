package server

import (
	"encoding/xml"
	"fmt"
	"net/http"
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

	metadata := extractMetadata(r)

	objInfo, err := s.storage.PutObject(bucket, key, r.Body, metadata, expectedChecksumSHA256)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrChecksumMismatch:
			s.errorResponse(w, r, "BadDigest", "The Content-SHA256 you specified did not match what we received.", http.StatusBadRequest)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", objInfo.ETag))
	w.Header().Set("x-amz-checksum-sha256", objInfo.ChecksumSHA256)
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GetObject operation
func (s *S3Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	reader, info, err := s.storage.GetObject(bucket, key)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrObjectNotFound:
			s.errorResponse(w, r, "NoSuchKey", "Object does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer reader.Close()

	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", info.ETag))
	w.Header().Set("x-amz-checksum-sha256", info.ChecksumSHA256)
	setMetadataHeaders(w, info.Metadata)

	http.ServeContent(w, r, key, info.ModTime, reader)
}

// handleDeleteObject handles DeleteObject operation
func (s *S3Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	err := s.storage.DeleteObject(bucket, key)
	if err != nil && err != storage.ErrObjectNotFound {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteObjects handles DeleteObjects operation (batch delete)
func (s *S3Handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	// Check if bucket exists
	if !s.storage.BucketExists(bucket) {
		s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		return
	}

	// Parse the request body
	var deleteReq Delete
	if err := xml.NewDecoder(r.Body).Decode(&deleteReq); err != nil {
		s.errorResponse(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
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
		s.errorResponse(w, r, "InvalidArgument", "Copy source header is required", http.StatusBadRequest)
		return
	}

	// Remove leading slash if present
	copySource = strings.TrimPrefix(copySource, "/")

	// Parse source bucket and key
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		s.errorResponse(w, r, "InvalidArgument", "Invalid copy source format", http.StatusBadRequest)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Handle x-amz-metadata-directive header
	// COPY (default): copy metadata from source object
	// REPLACE: use metadata from request headers
	metadataDirective := r.Header.Get("x-amz-metadata-directive")
	var metadata *storage.Metadata
	if metadataDirective == "REPLACE" {
		m := extractMetadata(r)
		metadata = &m
	}

	// Perform copy
	objInfo, err := s.storage.CopyObject(srcBucket, srcKey, dstBucket, dstKey, metadata)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrObjectNotFound:
			s.errorResponse(w, r, "NoSuchKey", "Source object does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := CopyObjectResult{
		LastModified: objInfo.ModTime.UTC(),
		ETag:         fmt.Sprintf("%q", objInfo.ETag),
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleRenameObject handles RenameObject operation
func (s *S3Handler) handleRenameObject(w http.ResponseWriter, r *http.Request, bucket, dstKey string) {
	// Parse x-amz-rename-source header
	renameSource := r.Header.Get("x-amz-rename-source")
	if renameSource == "" {
		s.errorResponse(w, r, "InvalidArgument", "Rename source header is required", http.StatusBadRequest)
		return
	}

	// Remove leading slash if present
	renameSource = strings.TrimPrefix(renameSource, "/")

	// Parse source bucket and key
	parts := strings.SplitN(renameSource, "/", 2)
	if len(parts) != 2 {
		s.errorResponse(w, r, "InvalidArgument", "Invalid rename source format", http.StatusBadRequest)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Verify both source and destination are in the same bucket
	if srcBucket != bucket {
		s.errorResponse(w, r, "InvalidArgument", "RenameObject requires source and destination to be in the same bucket", http.StatusBadRequest)
		return
	}

	// Perform rename
	err := s.storage.RenameObject(bucket, srcKey, dstKey)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrObjectNotFound:
			s.errorResponse(w, r, "NoSuchKey", "Source object does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
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
	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		parsed, err := strconv.Atoi(mk)
		if err != nil || parsed < 0 {
			s.errorResponse(w, r, "InvalidArgument", "Argument max-keys must be an integer between 0 and 2147483647", http.StatusBadRequest)
			return
		}
		maxKeys = parsed
	}

	// Handle maxKeys=0 special case
	var objects []storage.ObjectInfo
	var commonPrefixes []string
	var err error
	if maxKeys == 0 {
		// When maxKeys is 0, return empty result immediately (nil slices work fine)
	} else {
		// Fetch one extra object to determine if there are more results
		objects, commonPrefixes, err = s.storage.ListObjects(bucket, prefix, delimiter, marker, maxKeys+1)
	}
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Determine if results are truncated
	isTruncated := len(objects) > maxKeys
	var nextMarker string
	if isTruncated {
		// Remove the extra object
		objects = objects[:maxKeys]
		// Set next marker to the last object key
		if len(objects) > 0 {
			nextMarker = objects[len(objects)-1].Key
		}
	}

	result := ListBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		Delimiter:   delimiter,
		MaxKeys:     maxKeys,
		IsTruncated: isTruncated,
	}

	if isTruncated {
		result.NextMarker = nextMarker
	}

	for _, obj := range objects {
		result.Contents = append(result.Contents, Contents{
			Key:          obj.Key,
			LastModified: obj.ModTime,
			ETag:         fmt.Sprintf("%q", obj.ETag),
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	for _, cp := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{
			Prefix: cp,
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
	fetchOwner := query.Get("fetch-owner") == "true"
	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		parsed, err := strconv.Atoi(mk)
		if err != nil || parsed < 0 {
			s.errorResponse(w, r, "InvalidArgument", "Argument max-keys must be an integer between 0 and 2147483647", http.StatusBadRequest)
			return
		}
		maxKeys = parsed
	}

	// Determine the marker to use
	marker := ""
	if continuationToken != "" {
		marker = continuationToken
	} else if startAfter != "" {
		marker = startAfter
	}

	// Handle maxKeys=0 special case
	var objects []storage.ObjectInfo
	var commonPrefixes []string
	var err error
	if maxKeys == 0 {
		// When maxKeys is 0, return empty result immediately (nil slices work fine)
	} else {
		// Fetch one extra object to determine if there are more results
		objects, commonPrefixes, err = s.storage.ListObjects(bucket, prefix, delimiter, marker, maxKeys+1)
	}
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Determine if results are truncated
	isTruncated := len(objects) > maxKeys
	var nextContinuationToken string
	if isTruncated {
		// Remove the extra object
		objects = objects[:maxKeys]
		// Set next continuation token to the last object key
		if len(objects) > 0 {
			nextContinuationToken = objects[len(objects)-1].Key
		}
	}

	result := ListBucketResultV2{
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		KeyCount:          len(objects),
		IsTruncated:       isTruncated,
		StartAfter:        startAfter,
		ContinuationToken: continuationToken,
	}

	if isTruncated {
		result.NextContinuationToken = nextContinuationToken
	}

	for _, obj := range objects {
		content := Contents{
			Key:          obj.Key,
			LastModified: obj.ModTime,
			ETag:         fmt.Sprintf("%q", obj.ETag),
			Size:         obj.Size,
			StorageClass: "STANDARD",
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
			Prefix: cp,
		})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}
