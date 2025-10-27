package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wzshiming/s3d/pkg/server"
	"github.com/wzshiming/s3d/pkg/storage"
)

var tsV1 *testServerV1

// testServerV1 holds the components needed for integration testing with AWS SDK v1
type testServerV1 struct {
	tmpDir   string
	listener net.Listener
	srv      *http.Server
	client   *s3.S3
	ctx      context.Context
}

// setupTestServerV1 creates and starts a test server for AWS SDK v1
func setupTestServerV1(t *testing.T) *testServerV1 {
	// Create temporary directory for storage
	tmpDir := t.TempDir()

	// Create storage and server
	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	s3Server := server.NewS3Server(store, nil)

	// Start test server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	addr := listener.Addr().String()
	srv := &http.Server{Handler: s3Server.Handler()}

	go srv.Serve(listener)

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create S3 client using AWS SDK v1
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String("http://" + addr),
		Credentials:      credentials.NewStaticCredentials("test", "test", ""),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
	})
	if err != nil {
		srv.Shutdown(context.Background())
		listener.Close()
		t.Fatalf("Failed to create session: %v", err)
	}

	client := s3.New(sess)

	return &testServerV1{
		tmpDir:   tmpDir,
		listener: listener,
		srv:      srv,
		client:   client,
		ctx:      context.Background(),
	}
}

// cleanup shuts down the test server and cleans up resources
func (ts *testServerV1) cleanup() {
	ts.srv.Shutdown(context.Background())
	ts.listener.Close()
}
