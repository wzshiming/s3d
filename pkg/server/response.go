package server

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/wzshiming/s3d/pkg/storage"
)

// extractMetadata extracts user-defined metadata from request headers (x-amz-meta-* headers)
// AWS S3 normalizes metadata keys to lowercase
func extractMetadata(r *http.Request) storage.Metadata {
	var metadata storage.Metadata
	prefix := "x-amz-meta-"

	for name, values := range r.Header {
		nameLower := strings.ToLower(name)
		if strings.HasPrefix(nameLower, prefix) {
			// Extract the metadata key (everything after the prefix)
			key := nameLower[len(prefix):]
			if len(values) > 0 {
				if metadata.XAmzMeta == nil {
					metadata.XAmzMeta = make(map[string]string)
				}
				metadata.XAmzMeta[key] = values[0]
			}
		}
	}

	if cacheControl := r.Header.Get("Cache-Control"); cacheControl != "" {
		metadata.CacheControl = cacheControl
	}
	if contentDisposition := r.Header.Get("Content-Disposition"); contentDisposition != "" {
		metadata.ContentDisposition = contentDisposition
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		metadata.ContentType = contentType
	}

	return metadata
}

// setMetadataHeaders sets user-defined metadata headers on the response
func setMetadataHeaders(w http.ResponseWriter, metadata storage.Metadata) {
	if metadata.CacheControl != "" {
		w.Header().Set("Cache-Control", metadata.CacheControl)
	}
	if metadata.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", metadata.ContentDisposition)
	}
	if metadata.ContentType != "" {
		w.Header().Set("Content-Type", metadata.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	for key, value := range metadata.XAmzMeta {
		headerName := "x-amz-meta-" + key
		w.Header().Set(headerName, value)
	}
}

// setHeaders sets common headers on the response
func (s *S3Handler) setHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-bucket-region", s.region)
}

// xmlResponse writes an XML response
func (s *S3Handler) xmlResponse(w http.ResponseWriter, r *http.Request, data any, status int) {
	s.setHeaders(w, r)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	if _, err := w.Write([]byte(xml.Header)); err != nil {
		return
	}
	if err := xml.NewEncoder(w).Encode(data); err != nil {
		return
	}
}

// errorResponse writes an error response
func (s *S3Handler) errorResponse(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	err := Error{
		Code:    code,
		Message: message,
	}

	s.setHeaders(w, r)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	if _, writeErr := w.Write([]byte(xml.Header)); writeErr != nil {
		return
	}
	if encodeErr := xml.NewEncoder(w).Encode(err); encodeErr != nil {
		return
	}
}
