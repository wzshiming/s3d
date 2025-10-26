package server

import (
	"encoding/xml"
	"net/http"

	"github.com/wzshiming/s3d/pkg/s3types"
)

// xmlResponse writes an XML response
func (s *S3Server) xmlResponse(w http.ResponseWriter, data any, status int) {
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
func (s *S3Server) errorResponse(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	err := s3types.Error{
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
