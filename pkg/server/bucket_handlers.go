package server

import (
	"net/http"
	"strconv"
)

// handleListBuckets handles ListBuckets operation
func (s *S3Handler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// Parse pagination parameters
	prefix := query.Get("prefix")
	continuationToken := query.Get("continuation-token")
	maxBuckets := 10000 // AWS default
	if mb := query.Get("max-buckets"); mb != "" {
		if parsed, err := strconv.Atoi(mb); err == nil && parsed > 0 {
			maxBuckets = parsed
		}
	}

	// Fetch one extra bucket to determine if there are more results
	buckets, nextContinuationToken, err := s.storage.ListBuckets(prefix, continuationToken, maxBuckets)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          "local-user",
			DisplayName: "local-user",
		},
		Prefix:            prefix,
		ContinuationToken: nextContinuationToken,
	}

	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, Bucket{
			Name:         b.Name,
			CreationDate: b.ModTime,
		})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleCreateBucket handles CreateBucket operation
func (s *S3Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := s.storage.CreateBucket(bucket)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucket handles DeleteBucket operation
func (s *S3Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := s.storage.DeleteBucket(bucket)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleHeadBucket handles HeadBucket operation
func (s *S3Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !s.storage.BucketExists(bucket) {
		s.setHeaders(w, r)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	s.setHeaders(w, r)
	// Return directory-like headers for s3fs-fuse compatibility
	// This helps s3fs understand the bucket root as a directory
	w.Header().Set("Content-Type", "application/x-directory")
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}
