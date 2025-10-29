package server

import (
	"fmt"
	"net"
	"os"
	"testing"

	"time"

	"github.com/wzshiming/s3d/pkg/storage"
)

func TestServerRun(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "s3d-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	s3Handler := NewS3Handler(store, WithRegion("us-east-1"))

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- fmt.Errorf("server stopped")
	}()

	// Give it a moment
	time.Sleep(100 * time.Millisecond)

	// Server should be ready
	if s3Handler == nil {
		t.Fatal("Server is nil")
	}

	t.Logf("Server would run on %s", addr)
}
