package server

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/wzshiming/s3d/pkg/s3types"
	"github.com/wzshiming/s3d/pkg/storage"
)

// handleInitiateMultipartUpload handles InitiateMultipartUpload operation
func (s *S3Server) handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
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

	result := s3types.InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleUploadPart handles UploadPart operation
func (s *S3Server) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key, uploadID, partNumberStr string) {
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		s.errorResponse(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		return
	}

	etag, err := s.storage.UploadPart(bucket, key, uploadID, partNumber, r.Body)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrInvalidUploadID {
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		} else if err == storage.ErrInvalidPartNumber {
			s.errorResponse(w, r, "InvalidArgument", "Invalid part number", http.StatusBadRequest)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("ETag", fmt.Sprintf("%q", etag))
	w.WriteHeader(http.StatusOK)
}

// handleCompleteMultipartUpload handles CompleteMultipartUpload operation
func (s *S3Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	var req s3types.CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, r, "MalformedXML", "Invalid XML", http.StatusBadRequest)
		return
	}

	// Convert to storage parts
	var parts []storage.Part
	for _, p := range req.Parts {
		parts = append(parts, storage.Part{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	etag, err := s.storage.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrInvalidUploadID {
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     fmt.Sprintf("%q", etag),
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleAbortMultipartUpload handles AbortMultipartUpload operation
func (s *S3Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	err := s.storage.AbortMultipartUpload(bucket, key, uploadID)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrInvalidUploadID {
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleListMultipartUploads handles ListMultipartUploads operation
func (s *S3Server) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	maxUploads := 1000
	if mu := query.Get("max-uploads"); mu != "" {
		if parsed, err := strconv.Atoi(mu); err == nil {
			maxUploads = parsed
		}
	}

	uploads, err := s.storage.ListMultipartUploads(bucket, prefix, maxUploads)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.ListMultipartUploadsResult{
		Bucket:      bucket,
		MaxUploads:  maxUploads,
		IsTruncated: false,
	}

	for _, upload := range uploads {
		result.Uploads = append(result.Uploads, s3types.Upload{
			Key:          upload.Key,
			UploadId:     upload.UploadID,
			Initiated:    upload.Created,
			StorageClass: "STANDARD",
		})
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleListParts handles ListParts operation
func (s *S3Server) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key, uploadID string) {
	query := r.URL.Query()
	maxParts := 1000
	if mp := query.Get("max-parts"); mp != "" {
		if parsed, err := strconv.Atoi(mp); err == nil {
			maxParts = parsed
		}
	}

	parts, err := s.storage.ListParts(bucket, key, uploadID, maxParts)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else if err == storage.ErrInvalidUploadID {
			s.errorResponse(w, r, "NoSuchUpload", "Upload does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := s3types.ListPartsResult{
		Bucket:       bucket,
		Key:          key,
		UploadId:     uploadID,
		StorageClass: "STANDARD",
		MaxParts:     maxParts,
		IsTruncated:  false,
	}

	for _, part := range parts {
		result.Parts = append(result.Parts, s3types.CompletedPart{
			PartNumber:   part.PartNumber,
			LastModified: time.Now(),
			ETag:         fmt.Sprintf("%q", part.ETag),
			Size:         part.Size,
		})
	}

	s.xmlResponse(w, result, http.StatusOK)
}
