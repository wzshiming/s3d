package main

import (
	"flag"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/s3d/pkg/auth"
	"github.com/wzshiming/s3d/pkg/server"
	"github.com/wzshiming/s3d/pkg/storage"
)

// Config holds the server configuration
type Config struct {
	Addr        string
	DataDir     string
	Credentials string
	Region      string
}

// parseCredentials parses comma-separated credentials and adds them to the authenticator
func parseCredentials(credString string, authenticator *auth.AWS4Authenticator) error {
	if credString == "" {
		return nil
	}

	credList := strings.Split(credString, ",")
	for _, cred := range credList {
		parts := strings.SplitN(strings.TrimSpace(cred), ":", 2)
		if len(parts) == 2 {
			authenticator.AddCredentials(parts[0], parts[1])
			log.Printf("Added credentials for access key: %s", parts[0])
		}
	}
	return nil
}

// createServer creates and configures the S3 server
func createServer(cfg *Config) (http.Handler, error) {
	// Create storage
	store, err := storage.NewStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s := server.NewS3Handler(store, server.WithRegion(cfg.Region))
	if cfg.Credentials == "" {
		return s, nil
	}

	// Create authenticator
	authenticator := auth.NewAWS4Authenticator()

	// Add credentials if provided
	if err := parseCredentials(cfg.Credentials, authenticator); err != nil {
		return nil, err
	}

	// Create server
	return authenticator.AuthMiddleware(s), nil
}

func main() {
	addr := flag.String("addr", ":8080", "Server address")
	dataDir := flag.String("data", "./data", "Data directory for storage")
	credentials := flag.String("credentials", "", "Credentials in format accessKeyID:secretAccessKey (can specify multiple separated by comma)")
	region := flag.String("region", "us-east-1", "AWS region name")
	flag.Parse()

	cfg := &Config{
		Addr:        *addr,
		DataDir:     *dataDir,
		Credentials: *credentials,
		Region:      *region,
	}

	handler, err := createServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	log.Printf("Starting S3-compatible server on %s", cfg.Addr)
	log.Printf("Data directory: %s", cfg.DataDir)
	log.Printf("Region: %s", cfg.Region)

	if cfg.Credentials == "" {
		log.Printf("WARNING: Running without authentication (no credentials configured)")
	}

	headersList := make([]string, 0, len(headers))
	for h := range headers {
		headersList = append(headersList, h)
	}
	sort.Strings(headersList)

	// Add CORS support
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}), // Allow all origins
		handlers.AllowedMethods([]string{http.MethodHead, http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete}),
		handlers.AllowedHeaders(headersList), // Allow specific headers
	)
	handler = corsHandler(handler)
	handler = handlers.CombinedLoggingHandler(log.Writer(), handler)
	if err := http.ListenAndServe(cfg.Addr, handler); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

var headers = map[string]struct{}{
	"authorization":                {},
	"content-type":                 {},
	"content-encoding":             {},
	"amz-sdk-invocation-id":        {},
	"amz-sdk-request":              {},
	"x-amz-content-sha256":         {},
	"x-amz-decoded-content-length": {},
	"x-amz-date":                   {},
	"x-amz-user-agent":             {},
	"x-amz-checksum-mode":          {},
	"x-amz-checksum-crc32":         {},
	"x-amz-checksum-sha256":        {},
	"x-amz-sdk-checksum-algorithm": {},
	"x-amz-copy-source":            {},
	"x-amz-rename-source":          {},
	"x-amz-trailer":                {},
}
