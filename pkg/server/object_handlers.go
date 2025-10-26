package server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/s3d/pkg/s3types"
	"github.com/wzshiming/s3d/pkg/storage"
)

// handlePutObject handles PutObject operation
func (s *S3Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	etag, err := s.storage.PutObject(bucket, key, r.Body, contentType)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("ETag", fmt.Sprintf("%q", etag))
	w.WriteHeader(http.StatusOK)
}

// handleGetObject handles GetObject operation
func (s *S3Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	reader, info, err := s.storage.GetObject(bucket, key)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrObjectNotFound {
			s.errorResponse(w, r, "NoSuchKey", "Object does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size))
	w.Header().Set("ETag", fmt.Sprintf("%q", info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))

	if _, err := io.Copy(w, reader); err != nil {
		// Headers already sent, can't return error response
		// Log the error but continue
		return
	}
}

// handleHeadObject handles HeadObject operation
func (s *S3Server) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	reader, info, err := s.storage.GetObject(bucket, key)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			w.WriteHeader(http.StatusNotFound)
		} else if err == storage.ErrObjectNotFound {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	reader.Close()

	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size))
	w.Header().Set("ETag", fmt.Sprintf("%q", info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// handleDeleteObject handles DeleteObject operation
func (s *S3Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	err := s.storage.DeleteObject(bucket, key)
	if err != nil && err != storage.ErrObjectNotFound {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleCopyObject handles CopyObject operation
func (s *S3Server) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey string) {
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

	// Perform copy
	etag, err := s.storage.CopyObject(srcBucket, srcKey, dstBucket, dstKey)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrObjectNotFound {
			s.errorResponse(w, r, "NoSuchKey", "Source object does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.CopyObjectResult{
		LastModified: time.Now().UTC(),
		ETag:         fmt.Sprintf("%q", etag),
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleListObjects handles ListObjects operation (v1 and v2)
func (s *S3Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
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
		if parsed, err := strconv.Atoi(mk); err == nil {
			maxKeys = parsed
		}
	}

	objects, commonPrefixes, err := s.storage.ListObjects(bucket, prefix, delimiter, maxKeys)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.ListBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		Delimiter:   delimiter,
		MaxKeys:     maxKeys,
		IsTruncated: false,
	}

	for _, obj := range objects {
		result.Contents = append(result.Contents, s3types.Contents{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			ETag:         fmt.Sprintf("%q", obj.ETag),
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	for _, cp := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, s3types.CommonPrefix{
			Prefix: cp,
		})
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleListObjectsV2 handles ListObjectsV2 operation
func (s *S3Server) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	startAfter := query.Get("start-after")
	continuationToken := query.Get("continuation-token")
	maxKeys := 1000
	if mk := query.Get("max-keys"); mk != "" {
		if parsed, err := strconv.Atoi(mk); err == nil {
			maxKeys = parsed
		}
	}

	objects, commonPrefixes, err := s.storage.ListObjects(bucket, prefix, delimiter, maxKeys)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.ListBucketResultV2{
		Name:              bucket,
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeys,
		KeyCount:          len(objects),
		IsTruncated:       false,
		StartAfter:        startAfter,
		ContinuationToken: continuationToken,
	}

	for _, obj := range objects {
		result.Contents = append(result.Contents, s3types.Contents{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			ETag:         fmt.Sprintf("%q", obj.ETag),
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	for _, cp := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, s3types.CommonPrefix{
			Prefix: cp,
		})
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleRenameObject handles RenameObject operation
func (s *S3Server) handleRenameObject(w http.ResponseWriter, r *http.Request, bucket, sourceKey string) {
	// Get target key from query parameter
	targetKey := r.URL.Query().Get("targetKey")
	if targetKey == "" {
		s.errorResponse(w, r, "InvalidArgument", "targetKey query parameter is required", http.StatusBadRequest)
		return
	}

	// Perform rename
	etag, err := s.storage.RenameObject(bucket, sourceKey, targetKey)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrObjectNotFound {
			s.errorResponse(w, r, "NoSuchKey", "Source object does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.RenameObjectResult{
		LastModified: time.Now().UTC(),
		ETag:         fmt.Sprintf("%q", etag),
	}

	s.xmlResponse(w, result, http.StatusOK)
}
