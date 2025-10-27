package middleware

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestSanitizePath(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		expectError bool
	}{
		// Basic cases
		{
			name:     "empty path",
			input:    "",
			expected: "",
		},
		{
			name:     "root path",
			input:    "/",
			expected: "/",
		},
		{
			name:     "simple path",
			input:    "/bucket/key",
			expected: "/bucket/key",
		},
		// Non-ASCII characters (percent-encoded)
		{
			name:     "non-ascii characters - Chinese",
			input:    "/bucket/%E4%B8%AD%E6%96%87",
			expected: "/bucket/中文",
		},
		{
			name:     "non-ascii characters - Japanese",
			input:    "/bucket/%E6%97%A5%E6%9C%AC%E8%AA%9E",
			expected: "/bucket/日本語",
		},
		{
			name:     "non-ascii characters - emoji",
			input:    "/bucket/%F0%9F%98%80",
			expected: "/bucket/😀",
		},
		{
			name:     "non-ascii characters - mixed",
			input:    "/bucket/test%20%E4%B8%AD%E6%96%87%20file",
			expected: "/bucket/test 中文 file",
		},
		// Path normalization
		{
			name:     "double slashes",
			input:    "/bucket//key",
			expected: "/bucket/key",
		},
		{
			name:     "trailing slash",
			input:    "/bucket/key/",
			expected: "/bucket/key",
		},
		{
			name:     "dot segments",
			input:    "/bucket/./key",
			expected: "/bucket/key",
		},
		// Path traversal attempts
		{
			name:     "parent directory traversal",
			input:    "/bucket/../etc/passwd",
			expected: "/etc/passwd",
		},
		{
			name:     "multiple parent traversal",
			input:    "/bucket/../../etc/passwd",
			expected: "/etc/passwd",
		},
		{
			name:     "encoded parent directory",
			input:    "/bucket/%2E%2E/etc/passwd",
			expected: "/etc/passwd",
		},
		{
			name:     "current directory",
			input:    "/bucket/./key/./file",
			expected: "/bucket/key/file",
		},
		// Space encoding
		{
			name:     "space as plus - not decoded (plus is literal in path)",
			input:    "/bucket/key+with+plus",
			expected: "/bucket/key+with+plus",
		},
		{
			name:     "space as percent-20",
			input:    "/bucket/key%20with%20spaces",
			expected: "/bucket/key with spaces",
		},
		// Special characters
		{
			name:     "encoded special chars",
			input:    "/bucket/key%3Fquery",
			expected: "/bucket/key?query",
		},
		{
			name:     "encoded ampersand",
			input:    "/bucket/key%26value",
			expected: "/bucket/key&value",
		},
		// RFC 3986 unreserved characters (should not need encoding but can be)
		{
			name:     "encoded tilde",
			input:    "/bucket/%7Euser/file",
			expected: "/bucket/~user/file",
		},
		{
			name:     "encoded dash",
			input:    "/bucket/key%2Dname",
			expected: "/bucket/key-name",
		},
		// Complex real-world scenarios
		{
			name:     "s3 object key with special chars",
			input:    "/my-bucket/path/to/file%20(1)%20%5B2024%5D.txt",
			expected: "/my-bucket/path/to/file (1) [2024].txt",
		},
		{
			name:     "deeply nested path",
			input:    "/bucket/a/b/c/d/e/f/g/h/file.txt",
			expected: "/bucket/a/b/c/d/e/f/g/h/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sanitizePath(tt.input)
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestPathSanitizerMiddleware(t *testing.T) {
	tests := []struct {
		name           string
		requestPath    string
		expectedPath   string
		expectedStatus int
	}{
		{
			name:           "simple path",
			requestPath:    "/bucket/key",
			expectedPath:   "/bucket/key",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "non-ascii path",
			requestPath:    "/bucket/%E4%B8%AD%E6%96%87",
			expectedPath:   "/bucket/中文",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "path with spaces",
			requestPath:    "/bucket/key%20with%20spaces",
			expectedPath:   "/bucket/key with spaces",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "path traversal attempt",
			requestPath:    "/bucket/../etc/passwd",
			expectedPath:   "/etc/passwd",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "normalized path with dots",
			requestPath:    "/bucket/./key",
			expectedPath:   "/bucket/key",
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a handler that captures the path
			var capturedPath string
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedPath = r.URL.Path
				w.WriteHeader(http.StatusOK)
			})

			// Wrap with middleware
			middleware := NewPathSanitizer(handler)

			// Create test request
			req := httptest.NewRequest(http.MethodGet, tt.requestPath, nil)
			rec := httptest.NewRecorder()

			// Execute
			middleware.ServeHTTP(rec, req)

			// Verify status
			if rec.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			// Verify path was correctly sanitized
			if tt.expectedStatus == http.StatusOK && capturedPath != tt.expectedPath {
				t.Errorf("Expected path %q, got %q", tt.expectedPath, capturedPath)
			}
		})
	}
}

func TestPathSanitizerInvalidEncoding(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := NewPathSanitizer(handler)

	// Test with invalid percent encoding by manually creating requests
	invalidPaths := []struct {
		name string
		path string
	}{
		{"incomplete percent escape 1", "/bucket/key%"},
		{"incomplete percent escape 2", "/bucket/key%2"},
		{"invalid hex in percent escape", "/bucket/key%GG"},
	}

	for _, tc := range invalidPaths {
		t.Run(tc.name, func(t *testing.T) {
			// Create request manually to bypass httptest.NewRequest URL parsing
			req := &http.Request{
				Method: http.MethodGet,
				URL: &url.URL{
					Path: tc.path,
				},
			}
			rec := httptest.NewRecorder()

			middleware.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("Expected status %d for invalid encoding, got %d", http.StatusBadRequest, rec.Code)
			}
		})
	}
}
