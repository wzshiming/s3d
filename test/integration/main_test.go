package integration

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wzshiming/s3d/pkg/server"
	"github.com/wzshiming/s3d/pkg/storage"
)

var ts *testServer

func TestMain(m *testing.M) {
	// Setup test server
	ts = setupTestServer()

	// Run tests
	code := m.Run()

	// Cleanup test server
	ts.cleanup()

	// Exit with the test code
	os.Exit(code)
}

// testServer holds the components needed for integration testing
type testServer struct {
	tmpDir   string
	listener net.Listener
	srv      *http.Server
	client   *s3.Client
	ctx      context.Context
}

// setupTestServer creates and starts a test server
func setupTestServer() *testServer {
	// Create temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "s3d-test-*")
	if err != nil {
		panic(err)
	}

	// Create storage and server
	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		panic(err)
	}

	s3Handler := server.NewS3Handler(store, "us-east-1")

	// Start test server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	addr := listener.Addr().String()
	srv := &http.Server{Handler: s3Handler}

	go srv.Serve(listener)

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create S3 client
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               "http://" + addr,
					SigningRegion:     "us-east-1",
					HostnameImmutable: true,
				}, nil
			}),
		),
	)
	if err != nil {
		srv.Shutdown(context.Background())
		listener.Close()
		panic(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &testServer{
		tmpDir:   tmpDir,
		listener: listener,
		srv:      srv,
		client:   client,
		ctx:      ctx,
	}
}

// cleanup shuts down the test server and cleans up resources
func (ts *testServer) cleanup() {
	ts.srv.Shutdown(context.Background())
	ts.listener.Close()
	os.RemoveAll(ts.tmpDir)
}
