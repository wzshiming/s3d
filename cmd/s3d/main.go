package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/wzshiming/s3d/pkg/auth"
	"github.com/wzshiming/s3d/pkg/server"
	"github.com/wzshiming/s3d/pkg/storage"
)

// Config holds the server configuration
type Config struct {
	Addr        string
	DataDir     string
	Credentials string
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
	s := server.NewS3Handler(store)
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
	flag.Parse()

	cfg := &Config{
		Addr:        *addr,
		DataDir:     *dataDir,
		Credentials: *credentials,
	}

	handler, err := createServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	log.Printf("Starting S3-compatible server on %s", cfg.Addr)
	log.Printf("Data directory: %s", cfg.DataDir)

	if cfg.Credentials == "" {
		log.Printf("WARNING: Running without authentication (no credentials configured)")
	}

	if err := http.ListenAndServe(cfg.Addr, handler); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
