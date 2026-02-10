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

	metadata := extractMetadata(r)

	uploadID, err := s.storage.InitiateMultipartUpload(bucket, key, metadata)
	if err != nil {
		s.errorResponse(w, r, err)
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
		s.response(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		return
	}

	// Check if this is an UploadPartCopy request (has x-amz-copy-source header)
	if r.Header.Get("x-amz-copy-source") != "" {
		s.handleUploadPartCopy(w, r, bucket, key, uploadID, partNumber)
		return
	}

	// Get the expected checksum from the request header (if provided)
	expectedChecksumSHA256 := r.Header.Get("x-amz-checksum-sha256")
	expectedChecksumMD5 := r.Header.Get("x-amz-checksum-md5")
	if expectedChecksumMD5 == "" {
		expectedChecksumMD5 = r.Header.Get("Content-MD5")
	}

	objInfo, err := s.storage.UploadPart(bucket, key, uploadID, partNumber, r.Body, expectedChecksumSHA256, expectedChecksumMD5)
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

// handleUploadPartCopy handles UploadPartCopy operation
func (s *S3Handler) handleUploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string, partNumber int) {
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

	// URL decode the source key (S3 object keys in copy source can be URL-encoded)
	decodedSrcKey, err := url.QueryUnescape(srcKey)
	if err != nil {
		s.response(w, r, "InvalidArgument", "Invalid URL encoding in copy source", http.StatusBadRequest)
		return
	}

	// Parse x-amz-copy-source-range header (format: bytes=start-end)
	var startByte, endByte int64 = -1, -1
	copySourceRange := r.Header.Get("x-amz-copy-source-range")
	if copySourceRange != "" {
		startByte, endByte, err = parseByteRange(copySourceRange)
		if err != nil {
			s.response(w, r, "InvalidArgument", "Invalid copy source range format", http.StatusBadRequest)
			return
		}
	}

	// Perform copy to part
	objInfo, err := s.storage.UploadPartCopy(bucket, key, uploadID, partNumber, srcBucket, decodedSrcKey, startByte, endByte)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := CopyPartResult{
		LastModified: objInfo.ModTime.UTC(),
		ETag:         fmt.Sprintf("%q", objInfo.ETag),
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// parseByteRange parses a byte range string in the format "bytes=start-end"
// Returns start and end byte positions, or an error if the format is invalid
func parseByteRange(rangeHeader string) (int64, int64, error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return -1, -1, fmt.Errorf("invalid range format")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	rangeParts := strings.SplitN(rangeSpec, "-", 2)
	if len(rangeParts) != 2 {
		return -1, -1, fmt.Errorf("invalid range format")
	}

	startByte, err := strconv.ParseInt(rangeParts[0], 10, 64)
	if err != nil {
		return -1, -1, fmt.Errorf("invalid start byte")
	}

	endByte, err := strconv.ParseInt(rangeParts[1], 10, 64)
	if err != nil {
		return -1, -1, fmt.Errorf("invalid end byte")
	}

	return startByte, endByte, nil
}

// handleCompleteMultipartUpload handles CompleteMultipartUpload operation
func (s *S3Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	var req CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		s.response(w, r, "MalformedXML", "Invalid XML", http.StatusBadRequest)
		return
	}

	// Convert to storage parts
	parts := make([]storage.Multipart, 0, len(req.Parts))
	for _, p := range req.Parts {
		if len(parts) > 0 && parts[len(parts)-1].PartNumber+1 != p.PartNumber {
			s.response(w, r, "InvalidPartOrder", "Parts are not in ascending order", http.StatusBadRequest)
			return
		}

		parts = append(parts, storage.Multipart{
			PartNumber:     p.PartNumber,
			ETag:           p.ETag,
			ChecksumSHA256: p.ChecksumSHA256,
			ChecksumMD5:    p.ChecksumMD5,
		})
	}

	// Get the expected checksum from the request header (if provided)
	expectedChecksumSHA256 := r.Header.Get("x-amz-checksum-sha256")
	expectedChecksumMD5 := r.Header.Get("x-amz-checksum-md5")

	objInfo, err := s.storage.CompleteMultipartUpload(bucket, key, uploadID, parts, expectedChecksumSHA256, expectedChecksumMD5)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := CompleteMultipartUploadResult{
		Location:       fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:         bucket,
		Key:            key,
		ETag:           fmt.Sprintf("%q", objInfo.ETag),
		ChecksumSHA256: objInfo.ChecksumSHA256,
		ChecksumMD5:    objInfo.ChecksumMD5,
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleAbortMultipartUpload handles AbortMultipartUpload operation
func (s *S3Handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	err := s.storage.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		s.errorResponse(w, r, err)
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
	uploads, nextKeyMarker, nextUploadIDMarker, err := s.storage.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := ListMultipartUploadsResult{
		Bucket:             bucket,
		MaxUploads:         maxUploads,
		IsTruncated:        nextKeyMarker != "" || nextUploadIDMarker != "",
		KeyMarker:          keyMarker,
		UploadIdMarker:     uploadIDMarker,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIdMarker: nextUploadIDMarker,
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
	partNumberMarker := query.Get("part-number-marker")

	maxParts := 1000
	if mp := query.Get("max-parts"); mp != "" {
		if parsed, err := strconv.Atoi(mp); err == nil {
			maxParts = parsed
		}
	}

	// Fetch one extra part to determine if there are more results
	parts, nextPartNumberMarker, err := s.storage.ListParts(bucket, key, uploadID, partNumberMarker, maxParts)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := ListPartsResult{
		Bucket:               bucket,
		Key:                  key,
		UploadId:             uploadID,
		StorageClass:         "STANDARD",
		MaxParts:             maxParts,
		IsTruncated:          nextPartNumberMarker != "",
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextPartNumberMarker,
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
