package server

import (
	"encoding/xml"
	"time"
)

// Bucket represents a bucket in ListBuckets response
type Bucket struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

// Owner represents the owner of buckets
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// ListAllMyBucketsResult is the response for ListBuckets operation
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets struct {
		Bucket []Bucket `xml:"Bucket"`
	} `xml:"Buckets"`
	ContinuationToken string `xml:"ContinuationToken,omitempty"`
	Prefix            string `xml:"Prefix,omitempty"`
}

// Contents represents an object in ListObjectsV2 response
type Contents struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

// CommonPrefix represents a common prefix in ListObjectsV2 response
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// ListBucketResult is the response for ListObjects (v1) operation
type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker,omitempty"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []Contents     `xml:"Contents"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

// ListBucketResultV2 is the response for ListObjectsV2 operation
type ListBucketResultV2 struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	Contents              []Contents     `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

// InitiateMultipartUploadResult is the response for InitiateMultipartUpload operation
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// Multipart represents a part in CompleteMultipartUpload request
type Multipart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompletedPart represents a part in ListParts response
type CompletedPart struct {
	PartNumber   int       `xml:"PartNumber"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
}

// CompleteMultipartUpload is the request for CompleteMultipartUpload operation
type CompleteMultipartUpload struct {
	Parts []Multipart `xml:"Part"`
}

// CompleteMultipartUploadResult is the response for CompleteMultipartUpload operation
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// Upload represents an upload in ListMultipartUploads response
type Upload struct {
	Key          string    `xml:"Key"`
	UploadId     string    `xml:"UploadId"`
	Initiated    time.Time `xml:"Initiated"`
	StorageClass string    `xml:"StorageClass"`
}

// ListMultipartUploadsResult is the response for ListMultipartUploads operation
type ListMultipartUploadsResult struct {
	XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
	Bucket             string   `xml:"Bucket"`
	KeyMarker          string   `xml:"KeyMarker,omitempty"`
	UploadIdMarker     string   `xml:"UploadIdMarker,omitempty"`
	NextKeyMarker      string   `xml:"NextKeyMarker,omitempty"`
	NextUploadIdMarker string   `xml:"NextUploadIdMarker,omitempty"`
	MaxUploads         int      `xml:"MaxUploads"`
	IsTruncated        bool     `xml:"IsTruncated"`
	Uploads            []Upload `xml:"Upload"`
}

// ListPartsResult is the response for ListParts operation
type ListPartsResult struct {
	XMLName              xml.Name        `xml:"ListPartsResult"`
	Bucket               string          `xml:"Bucket"`
	Key                  string          `xml:"Key"`
	UploadId             string          `xml:"UploadId"`
	StorageClass         string          `xml:"StorageClass"`
	PartNumberMarker     int             `xml:"PartNumberMarker,omitempty"`
	NextPartNumberMarker int             `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int             `xml:"MaxParts"`
	IsTruncated          bool            `xml:"IsTruncated"`
	Parts                []CompletedPart `xml:"Part"`
}

// CopyObjectResult is the response for CopyObject operation
type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"CopyObjectResult"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

// CopyPartResult is the response for UploadPartCopy operation
type CopyPartResult struct {
	XMLName      xml.Name  `xml:"CopyPartResult"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

// Error represents an S3 error response
type Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// ObjectIdentifier represents an object to delete in DeleteObjects request
type ObjectIdentifier struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// Delete represents the delete request in DeleteObjects operation
type Delete struct {
	Objects []ObjectIdentifier `xml:"Object"`
	Quiet   bool               `xml:"Quiet,omitempty"`
}

// DeletedObject represents a successfully deleted object in DeleteObjects response
type DeletedObject struct {
	Key                   string `xml:"Key"`
	VersionId             string `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId string `xml:"DeleteMarkerVersionId,omitempty"`
}

// DeleteError represents an error deleting an object in DeleteObjects response
type DeleteError struct {
	Key       string `xml:"Key"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	VersionId string `xml:"VersionId,omitempty"`
}

// DeleteObjectsResult is the response for DeleteObjects operation
type DeleteObjectsResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted,omitempty"`
	Errors  []DeleteError   `xml:"Error,omitempty"`
}

// LoggingEnabled represents the logging configuration when enabled
type LoggingEnabled struct {
	TargetBucket string `xml:"TargetBucket"`
	TargetPrefix string `xml:"TargetPrefix,omitempty"`
}

// BucketLoggingStatus represents the logging status of a bucket
type BucketLoggingStatus struct {
	XMLName        xml.Name        `xml:"BucketLoggingStatus"`
	LoggingEnabled *LoggingEnabled `xml:"LoggingEnabled,omitempty"`
}
