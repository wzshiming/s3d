package server

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/wzshiming/s3d/pkg/storage"
)

func TestSelectObjectNotImplemented(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "s3d-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	handler := NewS3Handler(store, WithRegion("us-east-1"))

	// Create a test bucket first
	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Create a test object
	testData := []byte("Name,Age\nJohn,30\nJane,25\n")
	_, err = store.PutObject("test-bucket", "test.csv", bytes.NewReader(testData), storage.Metadata{}, "")
	if err != nil {
		t.Fatalf("Failed to put object: %v", err)
	}

	// Try to perform SelectObject operation
	requestBody := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM S3Object[*] s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
</SelectObjectContentRequest>`

	req := httptest.NewRequest(http.MethodPost, "/test-bucket/test.csv?select&select-type=2", bytes.NewReader([]byte(requestBody)))
	req.Header.Set("Content-Type", "application/xml")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Errorf("Expected status %d, got %d", http.StatusNotImplemented, w.Code)
	}

	// Verify the response is a proper S3 error
	var errResp Error
	if err := xml.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	if errResp.Code != "NotImplemented" {
		t.Errorf("Expected error code 'NotImplemented', got '%s'", errResp.Code)
	}

	if errResp.Message != "The SelectObject operation is not implemented" {
		t.Errorf("Expected error message about SelectObject, got '%s'", errResp.Message)
	}
}
