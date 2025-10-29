package server

import (
	"encoding/xml"
	"net/http"
	"strconv"

	"github.com/wzshiming/s3d/pkg/storage"
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
	buckets, err := s.storage.ListBuckets(prefix, continuationToken, maxBuckets+1)
	if err != nil {
		s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Determine if results are truncated
	isTruncated := len(buckets) > maxBuckets
	var nextContinuationToken string
	if isTruncated {
		// Remove the extra bucket
		buckets = buckets[:maxBuckets]
		// Set next continuation token to the last bucket name
		if len(buckets) > 0 {
			nextContinuationToken = buckets[len(buckets)-1].Name
		}
	}

	result := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          "local-user",
			DisplayName: "local-user",
		},
		Prefix: prefix,
	}
	
	if isTruncated {
		result.ContinuationToken = nextContinuationToken
	}

	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, Bucket{
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

	w.Header().Set("x-amz-bucket-region", s.storage.GetRegion())
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

	w.Header().Set("x-amz-bucket-region", s.storage.GetRegion())
	w.WriteHeader(http.StatusNoContent)
}

// handleHeadBucket handles HeadBucket operation
func (s *S3Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !s.storage.BucketExists(bucket) {
		w.Header().Set("x-amz-bucket-region", s.storage.GetRegion())
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("x-amz-bucket-region", s.storage.GetRegion())
	w.WriteHeader(http.StatusOK)
}

// handleGetBucketOwnershipControls handles GetBucketOwnershipControls operation
func (s *S3Handler) handleGetBucketOwnershipControls(w http.ResponseWriter, r *http.Request, bucket string) {
	ownership, err := s.storage.GetBucketOwnership(bucket)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := OwnershipControls{
		Rules: []OwnershipControlsRule{
			{ObjectOwnership: ownership},
		},
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handlePutBucketOwnershipControls handles PutBucketOwnershipControls operation
func (s *S3Handler) handlePutBucketOwnershipControls(w http.ResponseWriter, r *http.Request, bucket string) {
	var controls OwnershipControls
	if err := xml.NewDecoder(r.Body).Decode(&controls); err != nil {
		s.errorResponse(w, r, "MalformedXML", "Invalid XML", http.StatusBadRequest)
		return
	}

	if len(controls.Rules) == 0 {
		s.errorResponse(w, r, "MalformedXML", "At least one rule is required", http.StatusBadRequest)
		return
	}

	ownership := controls.Rules[0].ObjectOwnership
	if err := s.storage.PutBucketOwnership(bucket, ownership); err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("x-amz-bucket-region", s.storage.GetRegion())
	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucketOwnershipControls handles DeleteBucketOwnershipControls operation
func (s *S3Handler) handleDeleteBucketOwnershipControls(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.storage.DeleteBucketOwnership(bucket); err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("x-amz-bucket-region", s.storage.GetRegion())
	w.WriteHeader(http.StatusNoContent)
}
