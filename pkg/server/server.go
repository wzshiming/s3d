package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/wzshiming/s3d/pkg/server/accesslog"
	"github.com/wzshiming/s3d/pkg/storage"
)

// S3Handler represents the S3-compatible server
type S3Handler struct {
	storage          *storage.Storage
	region           string
	logFlusher       *accesslog.AccessLogFlusher
	logFlushInterval time.Duration
}

// Option is a functional option for configuring S3Handler
type Option func(*S3Handler)

// WithRegion sets the region for the S3Handler
func WithRegion(region string) Option {
	return func(h *S3Handler) {
		h.region = region
	}
}

// WithLogFlushInterval sets the interval for flushing access logs
func WithLogFlushInterval(d time.Duration) Option {
	return func(h *S3Handler) {
		h.logFlushInterval = d
	}
}

// NewS3Handler creates a new S3 server
func NewS3Handler(storage *storage.Storage, opts ...Option) *S3Handler {
	h := &S3Handler{
		storage:          storage,
		region:           "us-east-1", // default region
		logFlushInterval: 1 * time.Minute,
	}
	for _, opt := range opts {
		opt(h)
	}
	h.logFlusher = accesslog.NewAccessLogFlusher(storage, h.logFlushInterval)

	h.logFlusher.Init()
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
			s.notAllowedResponse(w, r)
		}
		return
	}

	bucket := parts[0]

	// Wrap response writer to capture status and bytes for access logging
	startTime := time.Now()
	rec := accesslog.NewResponseRecorder(w)
	defer func() {
		s.logFlusher.WriteAccessLog(bucket, r, rec, startTime)
	}()

	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	query := r.URL.Query()
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			if query.Has("logging") {
				s.handlePutBucketLogging(rec, r, bucket)
			} else {
				s.handleCreateBucket(rec, r, bucket)
			}
		case http.MethodGet:
			if query.Has("uploads") {
				s.handleListMultipartUploads(rec, r, bucket)
			} else if query.Has("logging") {
				s.handleGetBucketLogging(rec, r, bucket)
			} else {
				s.handleListObjects(rec, r, bucket)
			}
		case http.MethodPost:
			if query.Has("delete") {
				s.handleDeleteObjects(rec, r, bucket)
			} else if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				s.handlePostObject(rec, r, bucket)
			} else {
				s.notAllowedResponse(rec, r)
			}
		case http.MethodDelete:
			s.handleDeleteBucket(rec, r, bucket)
		case http.MethodHead:
			s.handleHeadBucket(rec, r, bucket)
		default:
			s.notAllowedResponse(rec, r)
		}
	} else {
		switch r.Method {
		case http.MethodPost:
			if query.Has("uploads") {
				s.handleInitiateMultipartUpload(rec, r, bucket, key)
			} else if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleCompleteMultipartUpload(rec, r, bucket, key, uploadID)
			} else {
				s.notAllowedResponse(rec, r)
			}
		case http.MethodPut:
			if query.Has("uploadId") {
				if partNumber := query.Get("partNumber"); partNumber != "" {
					uploadID := query.Get("uploadId")
					s.handleUploadPart(rec, r, bucket, key, uploadID, partNumber)
				} else {
					s.response(rec, r, "MissingParameter", "Missing partNumber parameter", http.StatusBadRequest)
				}
			} else {
				s.handlePutObject(rec, r, bucket, key)
			}
		case http.MethodGet:
			if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleListParts(rec, r, bucket, key, uploadID)
			} else {
				s.handleGetObject(rec, r, bucket, key)
			}
		case http.MethodHead:
			s.handleGetObject(rec, r, bucket, key)
		case http.MethodDelete:
			if query.Has("uploadId") {
				uploadID := query.Get("uploadId")
				s.handleAbortMultipartUpload(rec, r, bucket, key, uploadID)
			} else {
				s.handleDeleteObject(rec, r, bucket, key)
			}
		default:
			s.notAllowedResponse(rec, r)
		}
	}
}
