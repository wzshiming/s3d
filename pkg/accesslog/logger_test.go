package accesslog

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/s3d/pkg/storage"
)

func TestDefaultValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accesslog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	logger := NewLogger(store)
	defer logger.Close()

	if logger.cacheTTL != DefaultCacheTTL {
		t.Errorf("Expected default CacheTTL %v, got %v", DefaultCacheTTL, logger.cacheTTL)
	}
	if logger.maxBufferSize != DefaultMaxBufferSize {
		t.Errorf("Expected default MaxBufferSize %d, got %d", DefaultMaxBufferSize, logger.maxBufferSize)
	}
	if logger.flushInterval != DefaultFlushInterval {
		t.Errorf("Expected default FlushInterval %v, got %v", DefaultFlushInterval, logger.flushInterval)
	}
}

func TestNewLogger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accesslog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	logger := NewLogger(store)
	if logger == nil {
		t.Fatal("Expected logger to be created")
	}
	if logger.storage != store {
		t.Error("Logger storage not set correctly")
	}
	if logger.configCache == nil {
		t.Error("Logger configCache not initialized")
	}
	if logger.buffers == nil {
		t.Error("Logger buffers not initialized")
	}

	// Clean up
	logger.Close()
}

func TestNewLoggerWithOptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accesslog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	logger := NewLogger(store,
		WithCacheTTL(1*time.Minute),
		WithMaxBufferSize(50),
		WithFlushInterval(30*time.Minute),
	)
	if logger == nil {
		t.Fatal("Expected logger to be created")
	}
	if logger.cacheTTL != 1*time.Minute {
		t.Errorf("Expected CacheTTL 1m, got %v", logger.cacheTTL)
	}
	if logger.maxBufferSize != 50 {
		t.Errorf("Expected MaxBufferSize 50, got %d", logger.maxBufferSize)
	}
	if logger.flushInterval != 30*time.Minute {
		t.Errorf("Expected FlushInterval 30m, got %v", logger.flushInterval)
	}

	// Clean up
	logger.Close()
}

func TestLoggerClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accesslog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	logger := NewLogger(store)

	// Close should not error
	err = logger.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Close again should not panic
	err = logger.Close()
	if err != nil {
		t.Errorf("Second close returned error: %v", err)
	}
}

func TestInvalidateCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accesslog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	logger := NewLogger(store)
	defer logger.Close()

	// Add a cache entry manually
	logger.cacheMu.Lock()
	logger.configCache["test-bucket"] = &cachedLoggingConfig{
		config:    &storage.LoggingConfig{TargetBucket: "logs"},
		expiresAt: time.Now().Add(1 * time.Hour),
	}
	logger.cacheMu.Unlock()

	// Verify it exists
	logger.cacheMu.RLock()
	_, exists := logger.configCache["test-bucket"]
	logger.cacheMu.RUnlock()
	if !exists {
		t.Fatal("Cache entry not found")
	}

	// Invalidate
	logger.InvalidateCache("test-bucket")

	// Verify it's gone
	logger.cacheMu.RLock()
	_, exists = logger.configCache["test-bucket"]
	logger.cacheMu.RUnlock()
	if exists {
		t.Error("Cache entry should be removed")
	}
}

func TestFormatEntry(t *testing.T) {
	tests := []struct {
		name     string
		entry    *Entry
		contains []string
	}{
		{
			name: "Basic entry",
			entry: &Entry{
				Bucket:      "test-bucket",
				Key:         "test-key",
				RequestURI:  "/test-bucket/test-key",
				HTTPStatus:  200,
				BytesSent:   1024,
				ObjectSize:  512,
				TotalTime:   5,
				RemoteIP:    "192.168.1.1",
				UserAgent:   "test-agent",
				Timestamp:   time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
				Method:      "GET",
				BucketOwner: "owner123",
				Requester:   "user456",
			},
			contains: []string{
				"owner123",
				"user456",
				"test-bucket",
				"200",
				"1024",
				"512",
				"192.168.1.1",
				"test-agent",
				"REST.GET.OBJECT",
			},
		},
		{
			name: "Entry with missing fields",
			entry: &Entry{
				Bucket:     "test-bucket",
				RequestURI: "/test-bucket",
				HTTPStatus: 404,
				Timestamp:  time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
				Method:     "GET",
			},
			contains: []string{
				"test-bucket",
				"404",
				"-", // Should have dashes for missing fields
			},
		},
		{
			name: "PUT operation",
			entry: &Entry{
				Bucket:     "test-bucket",
				Key:        "file.txt",
				RequestURI: "/test-bucket/file.txt",
				HTTPStatus: 200,
				Timestamp:  time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
				Method:     "PUT",
			},
			contains: []string{
				"REST.PUT.OBJECT",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatEntry(tt.entry)

			for _, expected := range tt.contains {
				if !strings.Contains(result, expected) {
					t.Errorf("Expected log to contain %q, got:\n%s", expected, result)
				}
			}

			// Verify it ends with newline
			if !strings.HasSuffix(result, "\n") {
				t.Error("Log entry should end with newline")
			}
		})
	}
}

func TestBuildOperationString(t *testing.T) {
	tests := []struct {
		name     string
		entry    *Entry
		expected string
	}{
		{
			name: "GET object",
			entry: &Entry{
				Method:     "GET",
				Key:        "file.txt",
				RequestURI: "/bucket/file.txt",
			},
			expected: "REST.GET.OBJECT",
		},
		{
			name: "PUT object",
			entry: &Entry{
				Method:     "PUT",
				Key:        "file.txt",
				RequestURI: "/bucket/file.txt",
			},
			expected: "REST.PUT.OBJECT",
		},
		{
			name: "GET bucket (list)",
			entry: &Entry{
				Method:     "GET",
				Key:        "",
				RequestURI: "/bucket",
			},
			expected: "REST.GET.BUCKET",
		},
		{
			name: "PUT bucket (create)",
			entry: &Entry{
				Method:     "PUT",
				Key:        "",
				RequestURI: "/bucket",
			},
			expected: "REST.PUT.BUCKET",
		},
		{
			name: "GET bucket logging",
			entry: &Entry{
				Method:     "GET",
				Key:        "",
				RequestURI: "/bucket?logging",
			},
			expected: "REST.GET.LOGGING",
		},
		{
			name: "PUT multipart",
			entry: &Entry{
				Method:     "PUT",
				Key:        "file.txt",
				RequestURI: "/bucket/file.txt?uploadId=123&partNumber=1",
			},
			expected: "REST.PUT.PART",
		},
		{
			name: "DELETE object",
			entry: &Entry{
				Method:     "DELETE",
				Key:        "file.txt",
				RequestURI: "/bucket/file.txt",
			},
			expected: "REST.DELETE.OBJECT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildOperationString(tt.entry)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestLogWithoutLoggingEnabled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "accesslog-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create bucket without logging
	err = store.CreateBucket("test-bucket")
	if err != nil {
		t.Fatal(err)
	}

	logger := NewLogger(store)
	defer logger.Close()

	entry := &Entry{
		Bucket:     "test-bucket",
		RequestURI: "/test-bucket",
		HTTPStatus: 200,
		Timestamp:  time.Now(),
		Method:     "GET",
	}

	// This should not panic and should not create any logs
	logger.Log(entry)

	// Verify no buffers were created
	logger.bufferMu.Lock()
	bufferCount := len(logger.buffers)
	logger.bufferMu.Unlock()

	if bufferCount != 0 {
		t.Errorf("Expected no buffers, got %d", bufferCount)
	}
}

func TestResponseWriter(t *testing.T) {
	// Create a mock ResponseWriter
	mockWriter := newMockResponseWriter()

	rw := NewResponseWriter(mockWriter)

	// Test default status code
	if rw.StatusCode != 200 {
		t.Errorf("Expected default status 200, got %d", rw.StatusCode)
	}

	// Test WriteHeader
	rw.WriteHeader(404)
	if rw.StatusCode != 404 {
		t.Errorf("Expected status 404, got %d", rw.StatusCode)
	}

	// Test Write
	data := []byte("test data")
	n, err := rw.Write(data)
	if err != nil {
		t.Errorf("Write error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected %d bytes written, got %d", len(data), n)
	}
	if rw.BytesWritten != int64(len(data)) {
		t.Errorf("Expected BytesWritten %d, got %d", len(data), rw.BytesWritten)
	}

	// Test multiple writes
	n, err = rw.Write(data)
	if err != nil {
		t.Errorf("Second write error: %v", err)
	}
	if rw.BytesWritten != int64(len(data)*2) {
		t.Errorf("Expected BytesWritten %d, got %d", len(data)*2, rw.BytesWritten)
	}
}

// mockResponseWriter is a mock implementation of http.ResponseWriter for testing
type mockResponseWriter struct {
	headers    http.Header
	body       []byte
	statusCode int
}

func newMockResponseWriter() *mockResponseWriter {
	return &mockResponseWriter{
		headers: make(http.Header),
	}
}

func (m *mockResponseWriter) Header() http.Header {
	return m.headers
}

func (m *mockResponseWriter) Write(b []byte) (int, error) {
	m.body = append(m.body, b...)
	return len(b), nil
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
}
