package accesslog

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/s3d/pkg/storage"
)

// responseRecorder wraps http.ResponseWriter to capture the status code and bytes written
type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func NewResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += int64(n)
	return n, err
}

// AccessLogFlusher manages buffered access log writing
type AccessLogFlusher struct {
	storage *storage.Storage
	logDir  string

	stopCh   chan struct{}
	stopped  chan struct{}
	interval time.Duration

	loggingConfigCacheMut sync.RWMutex
	loggingConfigCache    map[string]*loggingConfig

	loggingFilesMut sync.RWMutex
	loggingFiles    map[string]*os.File
}

// NewAccessLogFlusher creates a new access log flusher that flushes logs at the given interval
func NewAccessLogFlusher(store *storage.Storage, interval time.Duration) *AccessLogFlusher {
	f := &AccessLogFlusher{
		storage:            store,
		logDir:             store.LogDir(),
		stopCh:             make(chan struct{}),
		stopped:            make(chan struct{}),
		interval:           interval,
		loggingConfigCache: map[string]*loggingConfig{},
		loggingFiles:       map[string]*os.File{},
	}
	go f.run()
	return f
}

// run is the background loop that flushes logs periodically
func (a *AccessLogFlusher) run() {
	defer close(a.stopped)
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.FlushAll()
		case <-a.stopCh:
			a.FlushAll()
			return
		}
	}
}

// stop stops the flusher and flushes remaining logs
func (a *AccessLogFlusher) Stop() {
	close(a.stopCh)
	<-a.stopped
}

// logFilePath returns the path to the log file for a given bucket
func (a *AccessLogFlusher) logFilePath(bucket string) string {
	return filepath.Join(a.logDir, bucket+".log")
}

// appendLog appends a log line to the bucket's log file
func (a *AccessLogFlusher) appendLog(bucket, line string) (err error) {
	a.loggingFilesMut.Lock()
	defer a.loggingFilesMut.Unlock()

	file := a.loggingFiles[bucket]
	if file == nil {
		path := a.logFilePath(bucket)
		file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		a.loggingFiles[bucket] = file
	}

	_, err = file.WriteString(line)
	if err != nil {
		return err
	}
	return nil
}

// FlushAll iterates over all buckets, checks their logging config,
// and writes any buffered logs to the target bucket as objects
func (a *AccessLogFlusher) FlushAll() {
	a.loggingFilesMut.RLock()
	buckets := make([]string, 0, len(a.loggingFiles))
	for bucket := range a.loggingFiles {
		buckets = append(buckets, bucket)
	}
	a.loggingFilesMut.RUnlock()

	for _, bucket := range buckets {
		a.flushBucket(bucket)
	}
}

// Init loads logging configurations for all buckets into the cache at startup
func (a *AccessLogFlusher) Init() {
	buckets, err := a.storage.ListBucketNames()
	if err != nil {
		return
	}

	for _, bucket := range buckets {
		loggingConfig, err := getLoggingConfigFromStorage(a.storage, bucket)
		if err != nil {
			continue
		}
		a.loggingConfigCache[bucket] = loggingConfig
	}
}

// UpdateLoggingConfig updates the logging configuration for a bucket in the cache
func (a *AccessLogFlusher) UpdateLoggingConfig(bucket string) error {
	loggingConfig, err := getLoggingConfigFromStorage(a.storage, bucket)
	if err != nil {
		if err == storage.ErrBucketLoggingNotFound {
			a.loggingConfigCacheMut.Lock()
			delete(a.loggingConfigCache, bucket)
			a.loggingConfigCacheMut.Unlock()
			return nil
		}
		return err
	}

	a.loggingConfigCacheMut.Lock()
	defer a.loggingConfigCacheMut.Unlock()
	if loggingConfig.TargetBucket == "" {
		delete(a.loggingConfigCache, bucket)
		return nil
	}
	a.loggingConfigCache[bucket] = loggingConfig
	return nil
}

func (a *AccessLogFlusher) getLoggingConfig(bucket string) *loggingConfig {
	a.loggingConfigCacheMut.RLock()
	defer a.loggingConfigCacheMut.RUnlock()
	cfg := a.loggingConfigCache[bucket]
	return cfg
}

// flushBucket flushes buffered logs for a single bucket
func (a *AccessLogFlusher) flushBucket(bucket string) error {
	cfg := a.getLoggingConfig(bucket)
	if cfg == nil {
		return nil
	}

	a.loggingFilesMut.Lock()

	file := a.loggingFiles[bucket]
	file.Close()
	delete(a.loggingFiles, bucket)

	path := a.logFilePath(bucket)
	newPath := path + ".flushing"
	err := os.Rename(path, newPath)
	if err != nil {
		a.loggingFilesMut.Unlock()
		return err
	}
	a.loggingFilesMut.Unlock()

	data, err := os.ReadFile(newPath)
	if err != nil {
		return err
	}

	// Compress log data with gzip
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}

	now := time.Now().UTC()
	logKey := fmt.Sprintf("%s%s-%s.gz", cfg.TargetPrefix, now.Format("2006-01-02-15-04-05"), fmt.Sprintf("%09d", now.UnixNano()%1e9))

	_, err = a.storage.PutObject(cfg.TargetBucket, logKey, bytes.NewReader(buf.Bytes()), storage.Metadata{ContentType: "application/gzip"}, "", "")
	if err != nil {
		return err
	}

	// Truncate the file after reading
	if err := os.Remove(newPath); err != nil {
		return err
	}

	return nil
}

// loggingConfig holds the parsed logging configuration for a bucket
type loggingConfig struct {
	TargetBucket string
	TargetPrefix string
}

// getLoggingConfigFromStorage retrieves and parses the logging configuration for a bucket
func getLoggingConfigFromStorage(store *storage.Storage, bucket string) (*loggingConfig, error) {
	bucketLogging, err := store.GetBucketLogging(bucket)
	if err != nil {
		return nil, err
	}

	if bucketLogging.TargetBucket == "" {
		return nil, err
	}

	return &loggingConfig{
		TargetBucket: bucketLogging.TargetBucket,
		TargetPrefix: bucketLogging.TargetPrefix,
	}, nil
}

// writeAccessLog buffers an S3 server access log record for later flushing
func (a *AccessLogFlusher) WriteAccessLog(bucket string, r *http.Request, rec *responseRecorder, startTime time.Time) {
	cfg := a.getLoggingConfig(bucket)
	if cfg == nil {
		return
	}

	now := time.Now().UTC()
	duration := now.Sub(startTime)

	// Build an S3-compatible access log line
	// Format based on: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
	remoteAddr := r.RemoteAddr
	if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
		remoteAddr = remoteAddr[:idx]
	}

	key := extractKey(r)
	if key == "" {
		key = "-"
	}

	operation := s3Operation(r)
	requestURI := fmt.Sprintf("%s %s %s", r.Method, r.RequestURI, r.Proto)

	logLine := fmt.Sprintf(`%s %s [%s] %s %s %s %s "%s" %d %s %d %d %d %d "-" "-" - - - - - - -
`,
		"local-user",                             // bucket owner
		bucket,                                   // bucket name
		now.Format("02/Jan/2006:15:04:05 -0700"), // time
		remoteAddr,                               // remote IP
		"local-user",                             // requester
		"-",                                      // request ID
		operation,                                // operation
		requestURI,                               // request-URI
		rec.statusCode,                           // HTTP status
		"-",                                      // S3 error code (- for no error)
		rec.bytesWritten,                         // BytesSent (response body bytes)
		r.ContentLength,                          // ObjectSize
		duration.Milliseconds(),                  // TotalTime (ms)
		duration.Milliseconds(),                  // TurnAroundTime (ms)
	)

	// Append to the bucket's log file for later flushing
	_ = a.appendLog(bucket, logLine)
}

// extractKey extracts the object key from the request path
func extractKey(r *http.Request) string {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}

// s3Operation returns the S3 operation name based on the request
func s3Operation(r *http.Request) string {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	hasKey := len(parts) > 1 && parts[1] != ""
	query := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		if hasKey {
			return "REST.GET.OBJECT"
		}
		if query.Has("logging") {
			return "REST.GET.LOGGING_STATUS"
		}
		if query.Has("uploads") {
			return "REST.GET.UPLOADS"
		}
		return "REST.GET.BUCKET"
	case http.MethodPut:
		if hasKey {
			if r.Header.Get("x-amz-copy-source") != "" {
				return "REST.COPY.OBJECT"
			}
			if query.Has("uploadId") {
				return "REST.PUT.PART"
			}
			return "REST.PUT.OBJECT"
		}
		if query.Has("logging") {
			return "REST.PUT.LOGGING_STATUS"
		}
		return "REST.PUT.BUCKET"
	case http.MethodDelete:
		if hasKey {
			return "REST.DELETE.OBJECT"
		}
		return "REST.DELETE.BUCKET"
	case http.MethodHead:
		if hasKey {
			return "REST.HEAD.OBJECT"
		}
		return "REST.HEAD.BUCKET"
	case http.MethodPost:
		if hasKey {
			if query.Has("uploads") {
				return "REST.POST.UPLOADS"
			}
			if query.Has("uploadId") {
				return "REST.POST.MULTI_OBJECT_DELETE"
			}
		}
		if query.Has("delete") {
			return "REST.POST.MULTI_OBJECT_DELETE"
		}
		return "REST.POST.OBJECT"
	}
	return "REST." + r.Method
}
