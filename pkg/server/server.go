package server

import (
	"net/http"
	"strings"

	"github.com/wzshiming/s3d/pkg/storage"
)

// S3Handler represents the S3-compatible server
type S3Handler struct {
	storage *storage.Storage
	region  string
}

// Option is a functional option for configuring S3Handler
type Option func(*S3Handler)

// WithRegion sets the region for the S3Handler
func WithRegion(region string) Option {
	return func(h *S3Handler) {
		h.region = region
	}
}

// NewS3Handler creates a new S3 server
func NewS3Handler(storage *storage.Storage, opts ...Option) *S3Handler {
	h := &S3Handler{
		storage: storage,
		region:  "us-east-1", // default region
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// handleRequest handles all S3 requests
func (s *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	// Root path - list buckets
	if path == "" || path == "/" {
		if r.Method == http.MethodGet {
			s.handleListBuckets(w, r)
		} else {
			s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	bucket := parts[0]
	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	// Normalize key: trim leading slashes (e.g., from URLs like /bucket//key or /bucket/)
	// This handles cases where s3fs-fuse requests /bucket// to access the root directory
	key = strings.TrimPrefix(key, "/")

	query := r.URL.Query()
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			s.handleCreateBucket(w, r, bucket)
		case http.MethodGet:
			if query.Has("uploads") {
				s.handleListMultipartUploads(w, r, bucket)
			} else {
				s.handleListObjects(w, r, bucket)
			}
		case http.MethodPost:
			if query.Has("delete") {
				s.handleDeleteObjects(w, r, bucket)
			} else {
				s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
			}
		case http.MethodDelete:
			s.handleDeleteBucket(w, r, bucket)
		case http.MethodHead:
			s.handleHeadBucket(w, r, bucket)
		default:
			s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
	} else {
		switch r.Method {
		case http.MethodPost:
			if query.Has("uploads") {
				s.handleInitiateMultipartUpload(w, r, bucket, key)
			} else if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleCompleteMultipartUpload(w, r, bucket, key, uploadID)
			} else {
				s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
			}
		case http.MethodPut:
			if query.Has("uploadId") {
				if partNumber := query.Get("partNumber"); partNumber != "" {
					uploadID := query.Get("uploadId")
					s.handleUploadPart(w, r, bucket, key, uploadID, partNumber)
				} else {
					s.errorResponse(w, r, "MissingParameter", "Missing partNumber parameter", http.StatusBadRequest)
				}
			} else {
				s.handlePutObject(w, r, bucket, key)
			}
		case http.MethodGet:
			if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleListParts(w, r, bucket, key, uploadID)
			} else {
				s.handleGetObject(w, r, bucket, key)
			}
		case http.MethodHead:
			s.handleGetObject(w, r, bucket, key)
		case http.MethodDelete:
			if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleAbortMultipartUpload(w, r, bucket, key, uploadID)
			} else {
				s.handleDeleteObject(w, r, bucket, key)
			}
		default:
			s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}
