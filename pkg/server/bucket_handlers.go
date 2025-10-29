package server

import (
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

	s.xmlResponse(w, r, result, http.StatusOK)
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

	s.setHeaders(w, r)
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
	w.WriteHeader(http.StatusOK)
}

// handleGetBucketLogging handles GetBucketLogging operation
func (s *S3Handler) handleGetBucketLogging(w http.ResponseWriter, r *http.Request, bucket string) {
	loggingConfig, err := s.storage.GetBucketLogging(bucket)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	result := BucketLoggingStatus{}
	if loggingConfig != nil {
		result.LoggingEnabled = &LoggingEnabled{
			TargetBucket: loggingConfig.TargetBucket,
			TargetPrefix: loggingConfig.TargetPrefix,
		}

		// Convert grants
		for _, grant := range loggingConfig.TargetGrants {
			targetGrant := TargetGrant{
				Permission: grant.Permission,
			}
			targetGrant.Grantee.Type = grant.GranteeType
			targetGrant.Grantee.ID = grant.GranteeID
			targetGrant.Grantee.EmailAddress = grant.GranteeEmail
			targetGrant.Grantee.URI = grant.GranteeURI

			result.LoggingEnabled.TargetGrants = append(result.LoggingEnabled.TargetGrants, targetGrant)
		}
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handlePutBucketLogging handles PutBucketLogging operation
func (s *S3Handler) handlePutBucketLogging(w http.ResponseWriter, r *http.Request, bucket string) {
	var loggingStatus BucketLoggingStatus
	if err := s.xmlDecode(r.Body, &loggingStatus); err != nil {
		s.errorResponse(w, r, "MalformedXML", "The XML provided was not well-formed", http.StatusBadRequest)
		return
	}

	var loggingConfig *storage.LoggingConfig
	if loggingStatus.LoggingEnabled != nil {
		loggingConfig = &storage.LoggingConfig{
			TargetBucket: loggingStatus.LoggingEnabled.TargetBucket,
			TargetPrefix: loggingStatus.LoggingEnabled.TargetPrefix,
		}

		// Convert grants
		for _, grant := range loggingStatus.LoggingEnabled.TargetGrants {
			loggingConfig.TargetGrants = append(loggingConfig.TargetGrants, storage.LoggingTargetGrant{
				GranteeType:  grant.Grantee.Type,
				GranteeID:    grant.Grantee.ID,
				GranteeEmail: grant.Grantee.EmailAddress,
				GranteeURI:   grant.Grantee.URI,
				Permission:   grant.Permission,
			})
		}
	}

	err := s.storage.PutBucketLogging(bucket, loggingConfig)
	if err != nil {
		if err == storage.ErrBucketNotFound {
			s.errorResponse(w, r, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
		} else {
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Invalidate cache for this bucket
	s.logger.InvalidateCache(bucket)

	w.WriteHeader(http.StatusOK)
}
