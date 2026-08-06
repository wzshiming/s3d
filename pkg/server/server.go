package server

import (
	"net/http"
	"strings"

	"github.com/wzshiming/s3d/pkg/auth"
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

// healthPaths are unauthenticated health check endpoints (MinIO-compatible)
var healthPaths = map[string]struct{}{
	"minio/health/live":  {},
	"minio/health/ready": {},
}

// handleRequest handles all S3 requests
func (s *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	// Health check endpoints do not require authentication
	if _, ok := healthPaths[path]; ok {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
		} else {
			s.notAllowedResponse(w, r)
		}
		return
	}

	anonymous := auth.IsAnonymous(r.Context())

	// Root path - list buckets
	if path == "" || path == "/" {
		if anonymous {
			s.response(w, r, "AccessDenied", "Access Denied", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet {
			s.handleListBuckets(w, r)
		} else {
			s.notAllowedResponse(w, r)
		}
		return
	}

	bucket := parts[0]
	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	// Anonymous requests: without ACL support, only reveal resource
	// existence. Nonexistent bucket/key returns 404; anything else 403.
	if anonymous {
		s.handleAnonymousRequest(w, r, bucket, key)
		return
	}

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
			} else if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				s.handlePostObject(w, r, bucket)
			} else {
				s.notAllowedResponse(w, r)
			}
		case http.MethodDelete:
			s.handleDeleteBucket(w, r, bucket)
		case http.MethodHead:
			s.handleHeadBucket(w, r, bucket)
		default:
			s.notAllowedResponse(w, r)
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
				s.notAllowedResponse(w, r)
			}
		case http.MethodPut:
			if query.Has("uploadId") {
				if partNumber := query.Get("partNumber"); partNumber != "" {
					uploadID := query.Get("uploadId")
					s.handleUploadPart(w, r, bucket, key, uploadID, partNumber)
				} else {
					s.response(w, r, "MissingParameter", "Missing partNumber parameter", http.StatusBadRequest)
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
			s.notAllowedResponse(w, r)
		}
	}
}

// handleAnonymousRequest handles requests from anonymous (unsigned) principals.
// Without ACL support, anonymous access is denied, but nonexistent resources
// still return 404 (NoSuchBucket/NoSuchKey) as AWS does.
func (s *S3Handler) handleAnonymousRequest(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if !s.storage.BucketExists(bucket) {
		s.errorResponse(w, r, storage.ErrBucketNotFound)
		return
	}

	if key != "" {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodDelete:
			if _, _, err := s.storage.GetObject(bucket, key); err != nil {
				s.errorResponse(w, r, err)
				return
			}
		}
	}

	s.response(w, r, "AccessDenied", "Access Denied", http.StatusForbidden)
}
