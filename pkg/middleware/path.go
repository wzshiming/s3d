// Package middleware provides HTTP middleware functions for request processing.
package middleware

import (
	"net/http"
	"net/url"
	"path"
	"strings"
)

// PathSanitizer is a middleware that sanitizes URL paths to ensure they are safe and properly decoded.
// It handles:
// - URL decoding (percent-encoded characters, including non-ASCII)
// - Path normalization (removing .., ., etc.)
// - Prevention of directory traversal attacks
// - RFC 3986 compliance
type PathSanitizer struct {
	next http.Handler
}

// NewPathSanitizer creates a new path sanitization middleware.
func NewPathSanitizer(next http.Handler) *PathSanitizer {
	return &PathSanitizer{
		next: next,
	}
}

// ServeHTTP implements the http.Handler interface.
func (p *PathSanitizer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Decode and sanitize the path
	sanitizedPath, err := sanitizePath(r.URL.Path)
	if err != nil {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Update the request with the sanitized path
	r.URL.Path = sanitizedPath

	// Call the next handler
	p.next.ServeHTTP(w, r)
}

// sanitizePath decodes and cleans a URL path according to RFC 3986.
// It performs the following operations:
// 1. URL decoding (handles percent-encoded characters)
// 2. Path normalization (removes . and .. segments)
// 3. Ensures the path doesn't escape the root directory
func sanitizePath(urlPath string) (string, error) {
	// If the path is empty or just "/", return as is
	if urlPath == "" || urlPath == "/" {
		return urlPath, nil
	}

	// Decode the URL path
	// Note: We use PathUnescape which is designed for path segments and
	// handles percent-encoded characters according to RFC 3986
	decodedPath, err := url.PathUnescape(urlPath)
	if err != nil {
		return "", err
	}

	// Clean the path to remove . and .. segments
	// This also handles multiple slashes and ensures the path is normalized
	cleanedPath := path.Clean(decodedPath)

	// Ensure the path doesn't try to escape the root
	// path.Clean() will convert paths like "/../etc" to "/etc"
	// but we want to ensure no path traversal is happening
	if strings.Contains(decodedPath, "..") {
		// Check if after cleaning, we're still trying to escape
		// This is a security measure to prevent directory traversal
		if !strings.HasPrefix(cleanedPath, "/") || strings.HasPrefix(cleanedPath, "..") {
			cleanedPath = "/" + strings.TrimPrefix(cleanedPath, "..")
		}
	}

	// Ensure the path starts with / if the original did
	if strings.HasPrefix(urlPath, "/") && !strings.HasPrefix(cleanedPath, "/") {
		cleanedPath = "/" + cleanedPath
	}

	// Special case: if Clean() returned ".", convert to "/"
	if cleanedPath == "." {
		cleanedPath = "/"
	}

	return cleanedPath, nil
}
