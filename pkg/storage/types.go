package storage

import "time"

// ObjectInfo contains metadata about an object
type ObjectInfo struct {
	Key         string
	Size        int64
	ETag        string
	ModTime     time.Time
	ContentType string
}

// BucketInfo contains metadata about a bucket
type BucketInfo struct {
	Name    string
	ModTime time.Time
}

// Multipart represents a part of a multipart upload
type Multipart struct {
	PartNumber int
	ETag       string
}

// Part represents a stored part of list parts
type Part struct {
	PartNumber int
	ETag       string
	Size       int64
	ModTime    time.Time
}

// MultipartUpload represents an in-progress multipart upload
type MultipartUpload struct {
	UploadID string
	Bucket   string
	Key      string
	ModTime  time.Time
}

// BucketMetadata represents bucket-level configuration
type BucketMetadata struct {
	LoggingEnabled *LoggingConfig `json:"loggingEnabled,omitempty"`
}

// LoggingConfig represents the bucket logging configuration
type LoggingConfig struct {
	TargetBucket string               `json:"targetBucket"`
	TargetPrefix string               `json:"targetPrefix,omitempty"`
	TargetGrants []LoggingTargetGrant `json:"targetGrants,omitempty"`
}

// LoggingTargetGrant represents a grant for log delivery
type LoggingTargetGrant struct {
	GranteeType  string `json:"granteeType"`
	GranteeID    string `json:"granteeId,omitempty"`
	GranteeEmail string `json:"granteeEmail,omitempty"`
	GranteeURI   string `json:"granteeUri,omitempty"`
	Permission   string `json:"permission"`
}
