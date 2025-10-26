package server

import (
	"net/http"

	"github.com/wzshiming/s3d/pkg/s3types"
	"github.com/wzshiming/s3d/pkg/storage"
)

// handleListBuckets handles ListBuckets operation
func (s *S3Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.storage.ListBuckets()
	if err != nil {
		s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	result := s3types.ListAllMyBucketsResult{
		Owner: s3types.Owner{
			ID:          "local-user",
			DisplayName: "local-user",
		},
	}

	for _, b := range buckets {
		result.Buckets.Bucket = append(result.Buckets.Bucket, s3types.Bucket{
			Name:         b.Name,
			CreationDate: b.CreationDate,
		})
	}

	s.xmlResponse(w, result, http.StatusOK)
}

// handleCreateBucket handles CreateBucket operation
func (s *S3Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
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
func (s *S3Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
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
func (s *S3Server) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !s.storage.BucketExists(bucket) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}
