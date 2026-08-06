package server

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
)

const (
	versioningConfigName = "versioning"
	taggingConfigName    = "tagging"
)

// bucketSubresources are the known bucket-level subresource query parameters
// that must not fall through to the plain bucket CRUD handlers.
var bucketSubresources = []string{
	"accelerate",
	"acl",
	"analytics",
	"cors",
	"encryption",
	"intelligent-tiering",
	"inventory",
	"lifecycle",
	"location",
	"logging",
	"metrics",
	"notification",
	"object-lock",
	"ownershipControls",
	"policy",
	"policyStatus",
	"publicAccessBlock",
	"replication",
	"requestPayment",
	"tagging",
	"versioning",
	"versions",
	"website",
}

// objectSubresources are the known object-level subresource query parameters
// that must not fall through to the plain object CRUD handlers.
var objectSubresources = []string{
	"acl",
	"attributes",
	"legal-hold",
	"restore",
	"retention",
	"select",
	"tagging",
	"torrent",
}

// findSubresource returns the first known subresource present in the query
func findSubresource(query url.Values, subresources []string) string {
	for _, sub := range subresources {
		if query.Has(sub) {
			return sub
		}
	}
	return ""
}

// handleBucketSubresource dispatches bucket subresource requests
func (s *S3Handler) handleBucketSubresource(w http.ResponseWriter, r *http.Request, bucket, subresource string) {
	if !s.storage.BucketExists(bucket) {
		s.response(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		return
	}

	switch subresource {
	case "versioning":
		switch r.Method {
		case http.MethodGet:
			s.handleGetBucketVersioning(w, r, bucket)
			return
		case http.MethodPut:
			s.handlePutBucketVersioning(w, r, bucket)
			return
		}
	case "tagging":
		switch r.Method {
		case http.MethodGet:
			s.handleGetBucketTagging(w, r, bucket)
			return
		case http.MethodPut:
			s.handlePutBucketTagging(w, r, bucket)
			return
		case http.MethodDelete:
			s.handleDeleteBucketTagging(w, r, bucket)
			return
		}
	case "acl":
		switch r.Method {
		case http.MethodGet:
			s.handleGetBucketACL(w, r, bucket)
			return
		case http.MethodPut:
			// ACLs are not supported; accept and ignore canned ACL requests
			s.setHeaders(w, r)
			w.WriteHeader(http.StatusOK)
			return
		}
	case "location":
		if r.Method == http.MethodGet {
			s.handleGetBucketLocation(w, r, bucket)
			return
		}
	case "policy":
		if r.Method == http.MethodGet {
			s.response(w, r, "NoSuchBucketPolicy", "The bucket policy does not exist", http.StatusNotFound)
			return
		}
	case "policyStatus":
		if r.Method == http.MethodGet {
			s.response(w, r, "NoSuchBucketPolicy", "The bucket policy does not exist", http.StatusNotFound)
			return
		}
	case "lifecycle":
		if r.Method == http.MethodGet {
			s.response(w, r, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist", http.StatusNotFound)
			return
		}
	case "encryption":
		if r.Method == http.MethodGet {
			s.response(w, r, "ServerSideEncryptionConfigurationNotFoundError", "The server side encryption configuration was not found", http.StatusNotFound)
			return
		}
	case "publicAccessBlock":
		if r.Method == http.MethodGet {
			s.response(w, r, "NoSuchPublicAccessBlockConfiguration", "The public access block configuration was not found", http.StatusNotFound)
			return
		}
	case "cors":
		if r.Method == http.MethodGet {
			s.response(w, r, "NoSuchCORSConfiguration", "The CORS configuration does not exist", http.StatusNotFound)
			return
		}
	}

	s.notImplementedResponse(w, r)
}

// handleObjectSubresource dispatches object subresource requests.
// Object subresource implementations are tracked separately; return
// NotImplemented so requests do not corrupt or alias object CRUD.
func (s *S3Handler) handleObjectSubresource(w http.ResponseWriter, r *http.Request, bucket, key, subresource string) {
	if !s.storage.BucketExists(bucket) {
		s.response(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		return
	}

	s.notImplementedResponse(w, r)
}

// handleGetBucketVersioning handles GetBucketVersioning operation
func (s *S3Handler) handleGetBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	data, err := s.storage.GetBucketConfig(bucket, versioningConfigName)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := VersioningConfiguration{}
	if len(data) > 0 {
		result.Status = string(data)
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handlePutBucketVersioning handles PutBucketVersioning operation
func (s *S3Handler) handlePutBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.response(w, r, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	var config VersioningConfiguration
	if err := xml.Unmarshal(body, &config); err != nil {
		s.response(w, r, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", http.StatusBadRequest)
		return
	}

	if config.Status != "Enabled" && config.Status != "Suspended" {
		s.response(w, r, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", http.StatusBadRequest)
		return
	}

	if err := s.storage.SetBucketConfig(bucket, versioningConfigName, []byte(config.Status)); err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusOK)
}

// handleGetBucketTagging handles GetBucketTagging operation
func (s *S3Handler) handleGetBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	data, err := s.storage.GetBucketConfig(bucket, taggingConfigName)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	if len(data) == 0 {
		s.response(w, r, "NoSuchTagSet", "The TagSet does not exist", http.StatusNotFound)
		return
	}

	var tagging Tagging
	if err := xml.Unmarshal(data, &tagging); err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.xmlResponse(w, r, tagging, http.StatusOK)
}

// handlePutBucketTagging handles PutBucketTagging operation
func (s *S3Handler) handlePutBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.response(w, r, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	var tagging Tagging
	if err := xml.Unmarshal(body, &tagging); err != nil {
		s.response(w, r, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema", http.StatusBadRequest)
		return
	}

	if err := s.storage.SetBucketConfig(bucket, taggingConfigName, body); err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteBucketTagging handles DeleteBucketTagging operation
func (s *S3Handler) handleDeleteBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.storage.DeleteBucketConfig(bucket, taggingConfigName); err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}

// handleGetBucketACL handles GetBucketAcl operation.
// ACLs are not supported; return a canned owner-full-control policy.
func (s *S3Handler) handleGetBucketACL(w http.ResponseWriter, r *http.Request, bucket string) {
	owner := Owner{
		ID:          "local-user",
		DisplayName: "local-user",
	}
	result := AccessControlPolicy{
		Owner: owner,
		AccessControlList: AccessControlList{
			Grants: []Grant{
				{
					Grantee: Grantee{
						XMLNSXSI:    "http://www.w3.org/2001/XMLSchema-instance",
						Type:        "CanonicalUser",
						ID:          owner.ID,
						DisplayName: owner.DisplayName,
					},
					Permission: "FULL_CONTROL",
				},
			},
		},
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handleGetBucketLocation handles GetBucketLocation operation
func (s *S3Handler) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	result := LocationConstraint{}
	// AWS returns an empty LocationConstraint for us-east-1
	if s.region != "us-east-1" {
		result.Location = s.region
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// notImplementedResponse writes a NotImplemented error response
func (s *S3Handler) notImplementedResponse(w http.ResponseWriter, r *http.Request) {
	s.response(w, r, "NotImplemented", "A header or query you provided implies functionality that is not implemented", http.StatusNotImplemented)
}
