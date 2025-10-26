package server

import (
	"net/http"
	"strings"

	"github.com/wzshiming/s3d/pkg/auth"
	"github.com/wzshiming/s3d/pkg/storage"
)

// S3Server represents the S3-compatible server
type S3Server struct {
	storage       *storage.Storage
	authenticator *auth.Authenticator
}

// NewS3Server creates a new S3 server
func NewS3Server(storage *storage.Storage, authenticator *auth.Authenticator) *S3Server {
	return &S3Server{
		storage:       storage,
		authenticator: authenticator,
	}
}

// Handler returns the HTTP handler
func (s *S3Server) Handler() http.Handler {
	mux := http.NewServeMux()
	var handler http.Handler = http.HandlerFunc(s.handleRequest)

	if s.authenticator != nil {
		handler = s.authenticator.AuthMiddleware(handler)
	}
	mux.Handle("/", handler)

	return mux
}

// handleRequest handles all S3 requests
func (s *S3Server) handleRequest(w http.ResponseWriter, r *http.Request) {
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

	// Check for multipart upload operations
	query := r.URL.Query()

	// List multipart uploads for a bucket
	if query.Has("uploads") && key == "" {
		s.handleListMultipartUploads(w, r, bucket)
		return
	}

	// Initiate multipart upload
	if query.Has("uploads") && key != "" {
		s.handleInitiateMultipartUpload(w, r, bucket, key)
		return
	}

	if uploadID := query.Get("uploadId"); uploadID != "" {
		if partNumber := query.Get("partNumber"); partNumber != "" {
			s.handleUploadPart(w, r, bucket, key, uploadID, partNumber)
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.handleListParts(w, r, bucket, key, uploadID)
		case http.MethodPost:
			s.handleCompleteMultipartUpload(w, r, bucket, key, uploadID)
		case http.MethodDelete:
			s.handleAbortMultipartUpload(w, r, bucket, key, uploadID)
		default:
			s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Bucket operations
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			s.handleCreateBucket(w, r, bucket)
		case http.MethodGet:
			s.handleListObjects(w, r, bucket)
		case http.MethodDelete:
			s.handleDeleteBucket(w, r, bucket)
		case http.MethodHead:
			s.handleHeadBucket(w, r, bucket)
		default:
			s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Object operations
	switch r.Method {
	case http.MethodPost:
		// Check for rename operation (targetKey query parameter)
		// Always call handleRenameObject for POST on objects
		s.handleRenameObject(w, r, bucket, key)
	case http.MethodPut:
		// Check for copy operation (x-amz-copy-source header)
		if r.Header.Get("x-amz-copy-source") != "" {
			s.handleCopyObject(w, r, bucket, key)
		} else {
			s.handlePutObject(w, r, bucket, key)
		}
	case http.MethodGet:
		s.handleGetObject(w, r, bucket, key)
	case http.MethodDelete:
		s.handleDeleteObject(w, r, bucket, key)
	case http.MethodHead:
		s.handleHeadObject(w, r, bucket, key)
	default:
		s.errorResponse(w, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
	}
}
