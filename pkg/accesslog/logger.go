package accesslog

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/s3d/pkg/storage"
)

// Entry represents a single access log entry
type Entry struct {
	Bucket     string
	Key        string
	RequestURI string
	HTTPStatus int
	ErrorCode  string
	BytesSent  int64
	ObjectSize int64
	TotalTime  int64
	RemoteIP   string
	UserAgent  string
	Timestamp  time.Time
	Method     string
}

// cachedLoggingConfig holds cached bucket logging configuration
type cachedLoggingConfig struct {
	config    *storage.LoggingConfig
	expiresAt time.Time
}

// logBuffer holds buffered log entries for a bucket
type logBuffer struct {
	entries       []string
	targetBucket  string
	targetPrefix  string
	lastFlushTime time.Time
}

const (
	// CacheTTL is how long to cache bucket logging configurations
	CacheTTL = 5 * time.Minute
	// MaxBufferSize is the maximum number of log entries to buffer before flushing
	MaxBufferSize = 100
	// FlushInterval is how often to flush logs regardless of buffer size
	FlushInterval = 1 * time.Hour
)

// Logger handles access logging with caching and batching
type Logger struct {
	storage *storage.Storage

	// In-memory cache for disk-based bucket logging configurations
	configCache map[string]*cachedLoggingConfig
	cacheMu     sync.RWMutex

	// In-memory buffers for log entries
	buffers  map[string]*logBuffer
	bufferMu sync.Mutex
}

// NewLogger creates a new access logger
func NewLogger(storage *storage.Storage) *Logger {
	logger := &Logger{
		storage:     storage,
		configCache: make(map[string]*cachedLoggingConfig),
		buffers:     make(map[string]*logBuffer),
	}

	// Start background log flusher
	logger.startFlusher()

	return logger
}

// Log writes an access log entry if logging is enabled for the bucket
func (l *Logger) Log(entry *Entry) {
	// Get cached bucket logging configuration
	config := l.getCachedConfig(entry.Bucket)
	if config == nil {
		// Logging not configured, skip
		return
	}

	// Format the log entry
	logLine := formatEntry(entry)

	// Add to buffer
	l.addToBuffer(entry.Bucket, config, logLine)
}

// InvalidateCache removes a bucket's logging configuration from cache
func (l *Logger) InvalidateCache(bucket string) {
	l.cacheMu.Lock()
	delete(l.configCache, bucket)
	l.cacheMu.Unlock()
}

// FlushAll flushes all buffered logs immediately (for testing)
func (l *Logger) FlushAll() {
	l.bufferMu.Lock()
	buffers := make(map[string]*logBuffer)
	for k, v := range l.buffers {
		buffers[k] = v
	}
	l.bufferMu.Unlock()

	// Flush each buffer synchronously for testing
	for bucket, buffer := range buffers {
		l.bufferMu.Lock()
		l.flushBufferSync(bucket, buffer)
		l.bufferMu.Unlock()
	}
}

// getCachedConfig retrieves bucket logging configuration from cache or storage
func (l *Logger) getCachedConfig(bucket string) *storage.LoggingConfig {
	l.cacheMu.RLock()
	cached, exists := l.configCache[bucket]
	l.cacheMu.RUnlock()

	// Return cached value if valid
	if exists && time.Now().Before(cached.expiresAt) {
		return cached.config
	}

	// Fetch from storage
	config, err := l.storage.GetBucketLogging(bucket)
	if err != nil || config == nil {
		// Cache the nil result too (for buckets without logging)
		l.cacheMu.Lock()
		l.configCache[bucket] = &cachedLoggingConfig{
			config:    nil,
			expiresAt: time.Now().Add(CacheTTL),
		}
		l.cacheMu.Unlock()
		return nil
	}

	// Cache the result
	l.cacheMu.Lock()
	l.configCache[bucket] = &cachedLoggingConfig{
		config:    config,
		expiresAt: time.Now().Add(CacheTTL),
	}
	l.cacheMu.Unlock()

	return config
}

// addToBuffer adds a log entry to the buffer and flushes if needed
func (l *Logger) addToBuffer(bucket string, config *storage.LoggingConfig, logLine string) {
	l.bufferMu.Lock()
	defer l.bufferMu.Unlock()

	// Get or create buffer for this bucket
	buffer, exists := l.buffers[bucket]
	if !exists {
		buffer = &logBuffer{
			entries:       make([]string, 0, MaxBufferSize),
			targetBucket:  config.TargetBucket,
			targetPrefix:  config.TargetPrefix,
			lastFlushTime: time.Now(),
		}
		l.buffers[bucket] = buffer
	}

	// Add entry to buffer
	buffer.entries = append(buffer.entries, logLine)

	// Check if we need to flush
	shouldFlush := len(buffer.entries) >= MaxBufferSize ||
		time.Since(buffer.lastFlushTime) >= FlushInterval

	if shouldFlush {
		l.flushBuffer(bucket, buffer)
	}
}

// flushBuffer writes buffered log entries to storage asynchronously
func (l *Logger) flushBuffer(bucket string, buffer *logBuffer) {
	if len(buffer.entries) == 0 {
		return
	}

	// Create a copy of entries to flush
	entries := make([]string, len(buffer.entries))
	copy(entries, buffer.entries)
	targetBucket := buffer.targetBucket
	targetPrefix := buffer.targetPrefix

	// Clear the buffer
	buffer.entries = buffer.entries[:0]
	buffer.lastFlushTime = time.Now()

	// Flush asynchronously
	go func() {
		// Combine all log entries
		var logContent bytes.Buffer
		for _, entry := range entries {
			logContent.WriteString(entry)
		}

		// Generate log file name with timestamp
		timestamp := time.Now().Format("2006-01-02-15-04-05")
		logKey := fmt.Sprintf("%s%s-%s.log",
			targetPrefix,
			timestamp,
			bucket)

		// Write to target bucket
		reader := bytes.NewReader(logContent.Bytes())
		_, err := l.storage.PutObject(
			targetBucket,
			logKey,
			reader,
			"text/plain",
		)
		if err != nil {
			// Log writing failed, but we don't want to fail the original request
			// In production, you might want to queue this for retry
			return
		}
	}()
}

// flushBufferSync writes buffered log entries to storage synchronously
func (l *Logger) flushBufferSync(bucket string, buffer *logBuffer) {
	if len(buffer.entries) == 0 {
		return
	}

	// Create a copy of entries to flush
	entries := make([]string, len(buffer.entries))
	copy(entries, buffer.entries)
	targetBucket := buffer.targetBucket
	targetPrefix := buffer.targetPrefix

	// Clear the buffer
	buffer.entries = buffer.entries[:0]
	buffer.lastFlushTime = time.Now()

	// Combine all log entries
	var logContent bytes.Buffer
	for _, entry := range entries {
		logContent.WriteString(entry)
	}

	// Generate log file name with timestamp
	timestamp := time.Now().Format("2006-01-02-15-04-05")
	logKey := fmt.Sprintf("%s%s-%s.log",
		targetPrefix,
		timestamp,
		bucket)

	// Write to target bucket
	reader := bytes.NewReader(logContent.Bytes())
	_, err := l.storage.PutObject(
		targetBucket,
		logKey,
		reader,
		"text/plain",
	)
	if err != nil {
		// Log writing failed, but we don't want to fail the original request
		return
	}
}

// startFlusher starts a background goroutine that periodically flushes log buffers
func (l *Logger) startFlusher() {
	ticker := time.NewTicker(FlushInterval)
	go func() {
		for range ticker.C {
			l.flushAll()
		}
	}()
}

// flushAll flushes all buffered logs
func (l *Logger) flushAll() {
	l.bufferMu.Lock()
	defer l.bufferMu.Unlock()

	for bucket, buffer := range l.buffers {
		if time.Since(buffer.lastFlushTime) >= FlushInterval {
			l.flushBuffer(bucket, buffer)
		}
	}
}

// formatEntry formats a log entry according to S3 access log format
func formatEntry(entry *Entry) string {
	// Simplified S3 access log format
	// Format: bucket-owner bucket [timestamp] remote-ip requester request-id operation bucket [request-uri]
	//         http-status error-code bytes-sent object-size total-time turn-around-time "referer" "user-agent"
	//         version-id host-id signature-version cipher-suite authentication-type host-header tls-version

	bucketOwner := "local-user"
	timestamp := entry.Timestamp.Format("02/Jan/2006:15:04:05 -0700")
	requester := "local-user"
	requestID := "-" // We don't track request IDs currently
	errorCode := "-"
	if entry.ErrorCode != "" {
		errorCode = entry.ErrorCode
	}

	// Format bytes sent and object size
	bytesSent := "-"
	if entry.BytesSent > 0 {
		bytesSent = fmt.Sprintf("%d", entry.BytesSent)
	}

	objectSize := "-"
	if entry.ObjectSize > 0 {
		objectSize = fmt.Sprintf("%d", entry.ObjectSize)
	}

	totalTimeMs := "-"
	if entry.TotalTime > 0 {
		totalTimeMs = fmt.Sprintf("%d", entry.TotalTime)
	}

	userAgent := "-"
	if entry.UserAgent != "" {
		userAgent = fmt.Sprintf("\"%s\"", entry.UserAgent)
	}

	// Build the operation string (e.g., REST.GET.OBJECT, REST.PUT.OBJECT)
	operation := buildOperationString(entry)

	return fmt.Sprintf("%s %s [%s] %s %s %s %s %s [%s] %d %s %s %s %s - - %s - - - - - - -\n",
		bucketOwner,
		entry.Bucket,
		timestamp,
		entry.RemoteIP,
		requester,
		requestID,
		operation,
		entry.Bucket,
		entry.RequestURI,
		entry.HTTPStatus,
		errorCode,
		bytesSent,
		objectSize,
		totalTimeMs,
		userAgent,
	)
}

// buildOperationString builds the operation string from the request
func buildOperationString(entry *Entry) string {
	method := entry.Method

	// Determine the operation type
	if entry.Key == "" {
		// Bucket-level operation
		switch method {
		case "GET":
			if strings.Contains(entry.RequestURI, "?logging") {
				return "REST.GET.LOGGING"
			} else if strings.Contains(entry.RequestURI, "?uploads") {
				return "REST.GET.UPLOADS"
			}
			return "REST.GET.BUCKET"
		case "PUT":
			if strings.Contains(entry.RequestURI, "?logging") {
				return "REST.PUT.LOGGING"
			}
			return "REST.PUT.BUCKET"
		case "DELETE":
			return "REST.DELETE.BUCKET"
		case "HEAD":
			return "REST.HEAD.BUCKET"
		case "POST":
			if strings.Contains(entry.RequestURI, "?delete") {
				return "REST.POST.MULTI_OBJECT_DELETE"
			}
			return "REST.POST.BUCKET"
		}
	} else {
		// Object-level operation
		switch method {
		case "GET":
			return "REST.GET.OBJECT"
		case "PUT":
			if strings.Contains(entry.RequestURI, "uploadId") {
				return "REST.PUT.PART"
			}
			return "REST.PUT.OBJECT"
		case "DELETE":
			if strings.Contains(entry.RequestURI, "uploadId") {
				return "REST.DELETE.UPLOAD"
			}
			return "REST.DELETE.OBJECT"
		case "HEAD":
			return "REST.HEAD.OBJECT"
		case "POST":
			if strings.Contains(entry.RequestURI, "uploads") {
				return "REST.POST.UPLOADS"
			} else if strings.Contains(entry.RequestURI, "uploadId") {
				return "REST.POST.UPLOAD"
			}
			return "REST.POST.OBJECT"
		}
	}

	return fmt.Sprintf("REST.%s.UNKNOWN", method)
}

// ResponseWriter wraps http.ResponseWriter to capture response details
type ResponseWriter struct {
	http.ResponseWriter
	StatusCode   int
	BytesWritten int64
}

// NewResponseWriter creates a new ResponseWriter
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		StatusCode:     http.StatusOK, // Default status
	}
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.BytesWritten += int64(n)
	return n, err
}
