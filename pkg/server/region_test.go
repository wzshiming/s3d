package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/s3d/pkg/storage"
)

// TestRegionHeader verifies that x-amz-bucket-region header is set in all responses
func TestRegionHeader(t *testing.T) {
	// Create temporary storage
	tmpDir := t.TempDir()
	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Test with custom region
	testRegion := "eu-west-1"
	handler := NewS3Handler(store, WithRegion(testRegion))

	// Create a bucket for testing
	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	tests := []struct {
		name           string
		method         string
		path           string
		body           []byte
		expectedStatus int
	}{
		{
			name:           "ListBuckets",
			method:         http.MethodGet,
			path:           "/",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "CreateBucket",
			method:         http.MethodPut,
			path:           "/new-bucket",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "HeadBucket",
			method:         http.MethodHead,
			path:           "/test-bucket",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "HeadBucket_NotFound",
			method:         http.MethodHead,
			path:           "/nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "ListObjects",
			method:         http.MethodGet,
			path:           "/test-bucket",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "PutObject",
			method:         http.MethodPut,
			path:           "/test-bucket/test-key",
			body:           []byte("test content"),
			expectedStatus: http.StatusOK,
		},
		{
			name:           "GetObject",
			method:         http.MethodGet,
			path:           "/test-bucket/test-key",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "HeadObject",
			method:         http.MethodHead,
			path:           "/test-bucket/test-key",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "DeleteObject",
			method:         http.MethodDelete,
			path:           "/test-bucket/test-key",
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "DeleteBucket",
			method:         http.MethodDelete,
			path:           "/new-bucket",
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "Error_NoSuchBucket",
			method:         http.MethodGet,
			path:           "/nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body *bytes.Reader
			if tt.body != nil {
				body = bytes.NewReader(tt.body)
			} else {
				body = bytes.NewReader([]byte{})
			}

			req := httptest.NewRequest(tt.method, tt.path, body)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			// Check status code
			if rec.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			// Check for region header
			regionHeader := rec.Header().Get("x-amz-bucket-region")
			if regionHeader != testRegion {
				t.Errorf("Expected region header %q, got %q", testRegion, regionHeader)
			}
		})
	}
}

// TestRegionHeaderDefaultValue verifies that the default region is used
func TestRegionHeaderDefaultValue(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Use default region
	defaultRegion := "us-east-1"
	handler := NewS3Handler(store, WithRegion(defaultRegion))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	regionHeader := rec.Header().Get("x-amz-bucket-region")
	if regionHeader != defaultRegion {
		t.Errorf("Expected default region %q, got %q", defaultRegion, regionHeader)
	}
}
