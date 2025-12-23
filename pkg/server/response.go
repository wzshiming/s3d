package server

import (
	"encoding/xml"
	"io"
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

// xmlDecode decodes XML from a reader
func (s *S3Handler) xmlDecode(r io.Reader, v any) error {
	return xml.NewDecoder(r).Decode(v)
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
