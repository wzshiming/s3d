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

// handleInitiateMultipartUpload handles InitiateMultipartUpload operation
func (s *S3Handler) handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	uploadID, err := s.storage.InitiateMultipartUpload(bucket, key, contentType)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleUploadPart handles UploadPart operation
func (s *S3Handler) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID, partNumberStr string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		s.errorResponse(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		return
	}

	// Check if this is an UploadPartCopy request (has x-amz-copy-source header)
	if r.Header.Get("x-amz-copy-source") != "" {
		s.handleUploadPartCopy(w, r, bucket, key, uploadID, partNumber)
		return
	}

	objInfo, err := s.storage.UploadPart(bucket, key, uploadID, partNumber, r.Body)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrInvalidUploadID:
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		case storage.ErrInvalidPartNumber:
			s.errorResponse(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", objInfo.ETag))
	w.Header().Set("x-amz-checksum-sha256", urlSafeToStdBase64(objInfo.ETag))
	w.WriteHeader(http.StatusOK)
}

// handleUploadPartCopy handles UploadPartCopy operation
func (s *S3Handler) handleUploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, partNumber int) {
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

	// URL decode the source key (S3 object keys in copy source can be URL-encoded)
	decodedSrcKey, err := url.QueryUnescape(srcKey)
	if err != nil {
		s.errorResponse(w, r, "InvalidArgument", "Invalid URL encoding in copy source", http.StatusBadRequest)
		return
	}

	// Perform copy to part
	objInfo, err := s.storage.UploadPartCopy(bucket, key, uploadID, partNumber, srcBucket, decodedSrcKey)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrObjectNotFound:
			s.errorResponse(w, r, "NoSuchKey", "Source object does not exist", http.StatusNotFound)
		case storage.ErrInvalidUploadID:
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		case storage.ErrInvalidPartNumber:
			s.errorResponse(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := CopyPartResult{
		LastModified: objInfo.ModTime.UTC(),
		ETag:         fmt.Sprintf("%q", objInfo.ETag),
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleCompleteMultipartUpload handles CompleteMultipartUpload operation
func (s *S3Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	var req CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, r, "MalformedXML", "Invalid XML", http.StatusBadRequest)
		return
	}

	// Convert to storage parts
	parts := make([]storage.Multipart, 0, len(req.Parts))
	for _, p := range req.Parts {
		if len(parts) > 0 && parts[len(parts)-1].PartNumber+1 != p.PartNumber {
			s.errorResponse(w, r, "InvalidPartOrder", "Parts are not in ascending order", http.StatusBadRequest)
			return
		}

		parts = append(parts, storage.Multipart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	objInfo, err := s.storage.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrInvalidUploadID:
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     fmt.Sprintf("%q", objInfo.ETag),
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleAbortMultipartUpload handles AbortMultipartUpload operation
func (s *S3Handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	err := s.storage.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrInvalidUploadID:
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleListMultipartUploads handles ListMultipartUploads operation
func (s *S3Handler) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	keyMarker := query.Get("key-marker")
	uploadIDMarker := query.Get("upload-id-marker")
	maxUploads := 1000
	if mu := query.Get("max-uploads"); mu != "" {
		if parsed, err := strconv.Atoi(mu); err == nil {
			maxUploads = parsed
		}
	}

	// Fetch one extra upload to determine if there are more results
	uploads, err := s.storage.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, maxUploads+1)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Determine if results are truncated
	isTruncated := len(uploads) > maxUploads
	var nextKeyMarker, nextUploadIDMarker string
	if isTruncated {
		// Remove the extra upload
		uploads = uploads[:maxUploads]
		// Set next markers to the last upload
		if len(uploads) > 0 {
			nextKeyMarker = uploads[len(uploads)-1].Key
			nextUploadIDMarker = uploads[len(uploads)-1].UploadID
		}
	}

	result := ListMultipartUploadsResult{
		Bucket:      bucket,
		MaxUploads:  maxUploads,
		IsTruncated: isTruncated,
		KeyMarker:   keyMarker,
	}

	if uploadIDMarker != "" {
		result.UploadIdMarker = uploadIDMarker
	}

	if isTruncated {
		result.NextKeyMarker = nextKeyMarker
		result.NextUploadIdMarker = nextUploadIDMarker
	}

	for _, upload := range uploads {
		result.Uploads = append(result.Uploads, Upload{
			Key:          upload.Key,
			UploadId:     upload.UploadID,
			Initiated:    upload.ModTime,
			StorageClass: "STANDARD",
		})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleListParts handles ListParts operation
func (s *S3Handler) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	query := r.URL.Query()
	partNumberMarker := 0
	if pnm := query.Get("part-number-marker"); pnm != "" {
		if parsed, err := strconv.Atoi(pnm); err == nil {
			partNumberMarker = parsed
		}
	}
	maxParts := 1000
	if mp := query.Get("max-parts"); mp != "" {
		if parsed, err := strconv.Atoi(mp); err == nil {
			maxParts = parsed
		}
	}

	// Fetch one extra part to determine if there are more results
	parts, err := s.storage.ListParts(bucket, key, uploadID, partNumberMarker, maxParts+1)
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		case storage.ErrInvalidUploadID:
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Determine if results are truncated
	isTruncated := len(parts) > maxParts
	var nextPartNumberMarker int
	if isTruncated {
		// Remove the extra part
		parts = parts[:maxParts]
		// Set next marker to the last part number
		if len(parts) > 0 {
			nextPartNumberMarker = parts[len(parts)-1].PartNumber
		}
	}

	result := ListPartsResult{
		Bucket:       bucket,
		Key:          key,
		UploadId:     uploadID,
		StorageClass: "STANDARD",
		MaxParts:     maxParts,
		IsTruncated:  isTruncated,
	}

	if partNumberMarker > 0 {
		result.PartNumberMarker = partNumberMarker
	}

	if isTruncated {
		result.NextPartNumberMarker = nextPartNumberMarker
	}

	for _, part := range parts {
		result.Parts = append(result.Parts, CompletedPart{
			PartNumber:   part.PartNumber,
			LastModified: part.ModTime,
			ETag:         fmt.Sprintf("%q", part.ETag),
			Size:         part.Size,
		})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}
