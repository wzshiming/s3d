package server

import (
	"encoding/xml"
	"net/http"
	"strings"
)

// urlSafeToStdBase64 converts URL-safe base64 encoding to standard base64 encoding.
// This is needed because we use URL-safe base64 (with - and _ characters) for filenames
// to avoid path separators, but AWS SDK expects standard base64 (with + and / characters)
// in HTTP headers like x-amz-checksum-sha256.
func urlSafeToStdBase64(urlSafe string) string {
	if urlSafe == "" {
		return ""
	}
	std := strings.ReplaceAll(urlSafe, "-", "+")
	std = strings.ReplaceAll(std, "_", "/")
	return std
}

// xmlResponse writes an XML response
func (s *S3Handler) xmlResponse(w http.ResponseWriter, data any, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	if _, err := w.Write([]byte(xml.Header)); err != nil {
		return
	}
	if err := xml.NewEncoder(w).Encode(data); err != nil {
		return
	}
}

// xmlRequest reads and decodes an XML request body
func (s *S3Handler) xmlRequest(r *http.Request, data any) error {
	return xml.NewDecoder(r.Body).Decode(data)
}

// errorResponse writes an error response
func (s *S3Handler) errorResponse(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	err := Error{
		Code:    code,
		Message: message,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	if _, writeErr := w.Write([]byte(xml.Header)); writeErr != nil {
		return
	}
	if encodeErr := xml.NewEncoder(w).Encode(err); encodeErr != nil {
		return
	}
}
