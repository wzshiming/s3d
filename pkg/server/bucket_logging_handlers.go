package server

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/wzshiming/s3d/pkg/storage"
)

// handleGetBucketLogging handles GetBucketLogging operation
func (s *S3Handler) handleGetBucketLogging(w http.ResponseWriter, r *http.Request, bucket string) {
	bucketLogging, err := s.storage.GetBucketLogging(bucket)
	if err != nil {
		if err == storage.ErrBucketLoggingNotFound {
			result := BucketLoggingStatus{}
			s.xmlResponse(w, r, result, http.StatusOK)
			return
		}
		s.errorResponse(w, r, err)
		return
	}

	result := BucketLoggingStatus{
		LoggingEnabled: &LoggingEnabled{
			TargetBucket: bucketLogging.TargetBucket,
			TargetPrefix: bucketLogging.TargetPrefix,
		},
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handlePutBucketLogging handles PutBucketLogging operation
func (s *S3Handler) handlePutBucketLogging(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		s.response(w, r, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	var loggingStatus BucketLoggingStatus
	if err := xml.Unmarshal(body, &loggingStatus); err != nil {
		s.response(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	if loggingStatus.LoggingEnabled == nil || loggingStatus.LoggingEnabled.TargetBucket == "" {
		err = s.storage.DeleteBucketLogging(bucket)
		if err != nil {
			s.errorResponse(w, r, err)
			return
		}
	} else {
		err = s.storage.PutBucketLogging(bucket, &storage.BucketLogging{
			TargetBucket: loggingStatus.LoggingEnabled.TargetBucket,
			TargetPrefix: loggingStatus.LoggingEnabled.TargetPrefix,
		})
		if err != nil {
			s.errorResponse(w, r, err)
			return
		}
	}

	err = s.logFlusher.UpdateLoggingConfig(bucket)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusOK)
}
