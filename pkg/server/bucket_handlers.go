package server

import (
	"net/http"
	"strconv"

	"github.com/wzshiming/s3d/pkg/s3types"
	"github.com/wzshiming/s3d/pkg/storage"
)

// handleListBuckets handles ListBuckets operation
func (s *S3Handler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	continuationToken := query.Get("continuation-token")
	maxBuckets := 1000
	if mb := query.Get("max-buckets"); mb != "" {
		if parsed, err := strconv.Atoi(mb); err == nil && parsed > 0 {
			maxBuckets = parsed
		}
	}

	// Fetch one extra bucket to determine if there are more results
	buckets, err := s.storage.ListBuckets(continuationToken, maxBuckets+1)
	if err != nil {
		s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	// Determine if results are truncated
	isTruncated := len(buckets) > maxBuckets
	var nextContinuationToken string
	if isTruncated {
		// Limit to maxBuckets
		buckets = buckets[:maxBuckets]
		// Set next continuation token to the last bucket name
		if len(buckets) > 0 {
			nextContinuationToken = buckets[len(buckets)-1].Name
		}
	}

	result := s3types.ListAllMyBucketsResult{
		Owner: s3types.Owner{
			ID:          "local-user",
			DisplayName: "local-user",
		},
		IsTruncated: isTruncated,
	}

	if isTruncated {
		result.ContinuationToken = nextContinuationToken
	}

	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, s3types.Bucket{
			Name:         b.Name,
			CreationDate: b.ModTime,
		})
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleCreateBucket handles CreateBucket operation
func (s *S3Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := s.storage.CreateBucket(bucket)
	if err != nil {
		if err == storage.ErrBucketAlreadyExists {
			s.errorResponse(w, r, "BucketAlreadyExists", "Bucket already exists", http.StatusConflict)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucket handles DeleteBucket operation
func (s *S3Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := s.storage.DeleteBucket(bucket)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHeadBucket handles HeadBucket operation
func (s *S3Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !s.storage.BucketExists(bucket) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}
