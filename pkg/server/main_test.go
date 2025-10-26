package server

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/wzshiming/s3d/pkg/storage"
)

var ts *testServer

func TestMain(m *testing.M) {
	// Setup test server
	ts = setupTestServer()

	// Run tests
	code := m.Run()

	// Cleanup
	ts.cleanup()

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

func setupTestServer() *testServer {
	// Create temp directory for storage
	tmpDir, err := os.MkdirTemp("", "s3d-test-*")
	if err != nil {
		panic(err)
	}

	// Create storage
	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		panic(err)
	}

	// Create server
	s3Server := NewS3Server(store, nil)

	// Start test HTTP server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	srv := &http.Server{Handler: s3Server.Handler()}
	ctx := context.Background()

	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// Create S3 client
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               "http://" + listener.Addr().String(),
			SigningRegion:     "us-east-1",
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithEndpointResolverWithOptions(customResolver),
	)

	if err != nil {
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

func (ts *testServer) cleanup() {
	ts.srv.Shutdown(ts.ctx)
	ts.listener.Close()
	os.RemoveAll(ts.tmpDir)
}
