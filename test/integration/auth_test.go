package integration

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wzshiming/s3d/pkg/auth"
	"github.com/wzshiming/s3d/pkg/server"
	"github.com/wzshiming/s3d/pkg/storage"
)

// TestAuthenticationRequired tests that authentication is enforced when configured
func TestAuthenticationRequired(t *testing.T) {
	// Setup test server with authentication
	tmpDir, err := os.MkdirTemp("", "s3d-auth-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	authenticator := auth.NewAWS4Authenticator()
	authenticator.AddCredentials("test-access-key", "test-secret-key")

	s3Handler := server.NewS3Handler(store, "us-east-1")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	srv := &http.Server{Handler: authenticator.AuthMiddleware(s3Handler)}

	go srv.Serve(listener)
	defer srv.Shutdown(context.Background())

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	// Test 1: Request without credentials should fail
	t.Run("NoCredentials", func(t *testing.T) {
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
			t.Fatalf("Failed to create config: %v", err)
		}

		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})

		_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err == nil {
			t.Fatal("Expected authentication error with anonymous credentials, got nil")
		}
	})

	// Test 2: Request with wrong credentials should fail
	t.Run("WrongCredentials", func(t *testing.T) {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				"wrong-access-key",
				"wrong-secret-key",
				"",
			)),
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
			t.Fatalf("Failed to create config: %v", err)
		}

		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})

		_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err == nil {
			t.Fatal("Expected authentication error with wrong credentials, got nil")
		}
	})

	// Test 3: Request with correct credentials should succeed
	t.Run("ValidCredentials", func(t *testing.T) {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion("us-east-1"),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				"test-access-key",
				"test-secret-key",
				"",
			)),
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
			t.Fatalf("Failed to create config: %v", err)
		}

		client := s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})

		// Should succeed with valid credentials
		output, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("Failed to list buckets with valid credentials: %v", err)
		}

		if output == nil {
			t.Fatal("Expected output, got nil")
		}
	})
}

// TestAuthenticatedBucketOperations tests that all bucket operations work with authentication
func TestAuthenticatedBucketOperations(t *testing.T) {
	// Setup test server with authentication
	tmpDir, err := os.MkdirTemp("", "s3d-auth-bucket-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	authenticator := auth.NewAWS4Authenticator()
	authenticator.AddCredentials("my-access-key", "my-secret-key")

	s3Handler := server.NewS3Handler(store, "us-east-1")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	srv := &http.Server{Handler: authenticator.AuthMiddleware(s3Handler)}

	go srv.Serve(listener)
	defer srv.Shutdown(context.Background())

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"my-access-key",
			"my-secret-key",
			"",
		)),
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
		t.Fatalf("Failed to create config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "test-authenticated-bucket"

	// Test: Create bucket
	t.Run("CreateBucket", func(t *testing.T) {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}
	})

	// Test: List buckets
	t.Run("ListBuckets", func(t *testing.T) {
		output, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if err != nil {
			t.Fatalf("Failed to list buckets: %v", err)
		}

		found := false
		for _, bucket := range output.Buckets {
			if *bucket.Name == bucketName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Created bucket not found in list")
		}
	})

	// Test: Head bucket
	t.Run("HeadBucket", func(t *testing.T) {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to head bucket: %v", err)
		}
	})

	// Test: Delete bucket
	t.Run("DeleteBucket", func(t *testing.T) {
		_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to delete bucket: %v", err)
		}
	})
}

// TestAuthenticatedObjectOperations tests that all object operations work with authentication
func TestAuthenticatedObjectOperations(t *testing.T) {
	// Setup test server with authentication
	tmpDir, err := os.MkdirTemp("", "s3d-auth-object-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	authenticator := auth.NewAWS4Authenticator()
	authenticator.AddCredentials("object-key", "object-secret")

	s3Handler := server.NewS3Handler(store, "us-east-1")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	srv := &http.Server{Handler: authenticator.AuthMiddleware(s3Handler)}

	go srv.Serve(listener)
	defer srv.Shutdown(context.Background())

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"object-key",
			"object-secret",
			"",
		)),
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
		t.Fatalf("Failed to create config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "test-object-bucket"
	objectKey := "test-object.txt"
	content := "Hello, authenticated S3!"

	// Create bucket first
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Test: Put object
	t.Run("PutObject", func(t *testing.T) {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}
	})

	// Test: Get object
	t.Run("GetObject", func(t *testing.T) {
		output, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}

		if string(data) != string(content) {
			t.Fatalf("Expected content %q, got %q", content, data)
		}
	})

	// Test: Head object
	t.Run("HeadObject", func(t *testing.T) {
		output, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to head object: %v", err)
		}

		if *output.ContentLength != int64(len(content)) {
			t.Fatalf("Expected content length %d, got %d", len(content), *output.ContentLength)
		}
	})

	// Test: List objects
	t.Run("ListObjects", func(t *testing.T) {
		output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Object not found in list")
		}
	})

	// Test: Delete object
	t.Run("DeleteObject", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete object: %v", err)
		}
	})
}

// TestMultipleCredentials tests that multiple sets of credentials can be configured
func TestMultipleCredentials(t *testing.T) {
	// Setup test server with multiple credentials
	tmpDir, err := os.MkdirTemp("", "s3d-multi-cred-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	authenticator := auth.NewAWS4Authenticator()
	authenticator.AddCredentials("user1-key", "user1-secret")
	authenticator.AddCredentials("user2-key", "user2-secret")

	s3Handler := server.NewS3Handler(store, "us-east-1")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	srv := &http.Server{Handler: authenticator.AuthMiddleware(s3Handler)}

	go srv.Serve(listener)
	defer srv.Shutdown(context.Background())

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	// Test both users can authenticate
	for _, creds := range []struct {
		accessKey string
		secretKey string
	}{
		{"user1-key", "user1-secret"},
		{"user2-key", "user2-secret"},
	} {
		t.Run("User_"+creds.accessKey, func(t *testing.T) {
			cfg, err := config.LoadDefaultConfig(ctx,
				config.WithRegion("us-east-1"),
				config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					creds.accessKey,
					creds.secretKey,
					"",
				)),
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
				t.Fatalf("Failed to create config: %v", err)
			}

			client := s3.NewFromConfig(cfg, func(o *s3.Options) {
				o.UsePathStyle = true
			})

			_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
			if err != nil {
				t.Fatalf("Failed to list buckets with credentials %s: %v", creds.accessKey, err)
			}
		})
	}
}
