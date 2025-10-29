package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/wzshiming/s3d/pkg/accesslog"
	"github.com/wzshiming/s3d/pkg/storage"
)

// S3Handler represents the S3-compatible server
type S3Handler struct {
	storage *storage.Storage
	region  string
	logger  *accesslog.Logger
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
		logger:  accesslog.NewLogger(storage),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// handleRequest handles all S3 requests
func (s *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Start timing for the request
	startTime := time.Now()

	// Wrap response writer to capture status and bytes
	lw := accesslog.NewResponseWriter(w)

	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	// Root path - list buckets
	if path == "" || path == "/" {
		if r.Method == http.MethodGet {
			s.handleListBuckets(lw, r)
		} else {
			s.errorResponse(lw, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	bucket := parts[0]
	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	// After handling the request, log it
	defer func() {
		totalTime := time.Since(startTime).Milliseconds()

		// Extract remote IP
		remoteIP := r.RemoteAddr
		if idx := strings.LastIndex(remoteIP, ":"); idx != -1 {
			remoteIP = remoteIP[:idx]
		}

		// Extract requester from Authorization header
		requester := extractRequester(r)

		entry := &accesslog.Entry{
			Bucket:      bucket,
			Key:         key,
			RequestURI:  r.RequestURI,
			HTTPStatus:  lw.StatusCode,
			BytesSent:   lw.BytesWritten,
			TotalTime:   totalTime,
			RemoteIP:    remoteIP,
			UserAgent:   r.UserAgent(),
			Timestamp:   startTime,
			Method:      r.Method,
			BucketOwner: bucket, // For now, use bucket name as owner
			Requester:   requester,
		}

		s.logger.Log(entry)
	}()

	query := r.URL.Query()
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			if query.Has("logging") {
				s.handlePutBucketLogging(lw, r, bucket)
			} else {
				s.handleCreateBucket(lw, r, bucket)
			}
		case http.MethodGet:
			if query.Has("logging") {
				s.handleGetBucketLogging(lw, r, bucket)
			} else if query.Has("uploads") {
				s.handleListMultipartUploads(lw, r, bucket)
			} else {
				s.handleListObjects(lw, r, bucket)
			}
		case http.MethodPost:
			if query.Has("delete") {
				s.handleDeleteObjects(lw, r, bucket)
			} else {
				s.errorResponse(lw, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
			}
		case http.MethodDelete:
			s.handleDeleteBucket(lw, r, bucket)
		case http.MethodHead:
			s.handleHeadBucket(lw, r, bucket)
		default:
			s.errorResponse(lw, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
	} else {
		switch r.Method {
		case http.MethodPost:
			if query.Has("uploads") {
				s.handleInitiateMultipartUpload(lw, r, bucket, key)
			} else if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleCompleteMultipartUpload(lw, r, bucket, key, uploadID)
			} else {
				s.errorResponse(lw, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
			}
		case http.MethodPut:
			if query.Has("uploadId") {
				if partNumber := query.Get("partNumber"); partNumber != "" {
					uploadID := query.Get("uploadId")
					s.handleUploadPart(lw, r, bucket, key, uploadID, partNumber)
				} else {
					s.errorResponse(lw, r, "MissingParameter", "Missing partNumber parameter", http.StatusBadRequest)
				}
			} else {
				s.handlePutObject(lw, r, bucket, key)
			}
		case http.MethodGet:
			if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleListParts(lw, r, bucket, key, uploadID)
			} else {
				s.handleGetObject(lw, r, bucket, key)
			}
		case http.MethodHead:
			s.handleGetObject(lw, r, bucket, key)
		case http.MethodDelete:
			if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleAbortMultipartUpload(lw, r, bucket, key, uploadID)
			} else {
				s.handleDeleteObject(lw, r, bucket, key)
			}
		default:
			s.errorResponse(lw, r, "MethodNotAllowed", "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// extractRequester extracts the requester ID from the Authorization header
func extractRequester(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	// Parse AWS Signature Version 4
	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		// Format: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, ...
		parts := strings.Split(authHeader, " ")
		if len(parts) < 2 {
			return ""
		}

		// Extract credential part
		for _, part := range strings.Split(parts[1], ",") {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "Credential=") {
				credential := strings.TrimPrefix(part, "Credential=")
				// Credential format: accessKeyID/date/region/service/aws4_request
				credParts := strings.Split(credential, "/")
				if len(credParts) > 0 {
					return credParts[0] // Return the access key ID
				}
			}
		}
	}

	return ""
}

// FlushLogs flushes all buffered access logs (for testing)
func (s *S3Handler) FlushLogs() {
	s.logger.FlushAll()
}
