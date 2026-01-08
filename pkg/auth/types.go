package auth

import (
	"encoding/xml"
)

// Error represents an S3 error response
type Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// AuthError represents an authentication error with specific error code
type AuthError struct {
	Code    string
	Message string
}

func (e *AuthError) Error() string {
	return e.Message
}

// NewAuthError creates a new authentication error with AWS S3 error code.
// Common error codes include:
//   - InvalidAccessKeyId: The AWS access key ID does not exist
//   - SignatureDoesNotMatch: The request signature does not match
//   - InvalidArgument: Invalid request parameters
//   - AccessDenied: Access denied for other reasons
func NewAuthError(code, message string) *AuthError {
	return &AuthError{
		Code:    code,
		Message: message,
	}
}
