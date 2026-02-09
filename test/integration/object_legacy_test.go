package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
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

// hmacSHA256 computes HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// createPresignedPostFields creates the form fields needed for a presigned POST upload.
func createPresignedPostFields(accessKey, secretKey, region, bucket, key string, expiration time.Time, conditions []interface{}) (map[string]string, error) {
	now := time.Now().UTC()
	dateStr := now.Format("20060102")
	dateTimeStr := now.Format("20060102T150405Z")
	credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", accessKey, dateStr, region)

	// Build policy
	policy := map[string]interface{}{
		"expiration": expiration.UTC().Format("2006-01-02T15:04:05Z"),
		"conditions": append(conditions,
			map[string]string{"bucket": bucket},
			map[string]string{"X-Amz-Algorithm": "AWS4-HMAC-SHA256"},
			map[string]string{"X-Amz-Credential": credential},
			map[string]string{"X-Amz-Date": dateTimeStr},
		),
	}

	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}

	policyBase64 := base64.StdEncoding.EncodeToString(policyJSON)

	// Calculate signature
	signingKey := auth.CalculateSigningKey(secretKey, dateStr, region, "s3")
	sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(policyBase64)))

	return map[string]string{
		"key":              key,
		"policy":           policyBase64,
		"X-Amz-Algorithm":  "AWS4-HMAC-SHA256",
		"X-Amz-Credential": credential,
		"X-Amz-Date":       dateTimeStr,
		"X-Amz-Signature":  sig,
	}, nil
}

// buildMultipartForm builds a multipart/form-data request body with the given fields and file content.
func buildMultipartForm(fields map[string]string, fileContent string) (*bytes.Buffer, string) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Write form fields first (must come before file)
	for k, v := range fields {
		_ = writer.WriteField(k, v)
	}

	// Write file field
	part, _ := writer.CreateFormFile("file", "upload.txt")
	io.WriteString(part, fileContent)

	writer.Close()
	return &buf, writer.FormDataContentType()
}

// setupAuthenticatedServer creates a test server with authentication and returns the address, client, and cleanup func.
func setupAuthenticatedServer(t *testing.T, accessKey, secretKey string) (string, *s3.Client, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "s3d-post-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create storage: %v", err)
	}

	authenticator := auth.NewAWS4Authenticator()
	authenticator.AddCredentials(accessKey, secretKey)

	s3Handler := server.NewS3Handler(store,
		server.WithRegion("us-east-1"),
	)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create listener: %v", err)
	}

	addr := listener.Addr().String()
	srv := &http.Server{Handler: authenticator.AuthMiddleware(s3Handler)}

	go srv.Serve(listener)

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
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
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	cleanup := func() {
		srv.Shutdown(context.Background())
		listener.Close()
		os.RemoveAll(tmpDir)
	}

	return addr, client, cleanup
}

func TestPresignedPostUpload(t *testing.T) {
	accessKey := "post-access-key"
	secretKey := "post-secret-key"

	addr, client, cleanup := setupAuthenticatedServer(t, accessKey, secretKey)
	defer cleanup()

	ctx := context.Background()
	bucketName := "post-upload-bucket"

	// Create bucket first
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	t.Run("BasicPostUpload", func(t *testing.T) {
		objectKey := "test-post-object.txt"
		content := "Hello from presigned POST!"
		expiration := time.Now().Add(1 * time.Hour)

		conditions := []interface{}{
			map[string]string{"key": objectKey},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, objectKey, expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 204, got %d: %s", resp.StatusCode, string(respBody))
		}

		// Verify the object was created
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get uploaded object: %v", err)
		}
		defer getOut.Body.Close()

		data, _ := io.ReadAll(getOut.Body)
		if string(data) != content {
			t.Errorf("Expected content %q, got %q", content, string(data))
		}

		// Cleanup
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	})

	t.Run("PostUploadWithStatus201", func(t *testing.T) {
		objectKey := "test-post-201.txt"
		content := "Hello with 201!"
		expiration := time.Now().Add(1 * time.Hour)

		conditions := []interface{}{
			map[string]string{"key": objectKey},
			map[string]string{"success_action_status": "201"},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, objectKey, expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}
		fields["success_action_status"] = "201"

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 201, got %d: %s", resp.StatusCode, string(respBody))
		}

		// Verify response contains XML with bucket, key, ETag
		respBody, _ := io.ReadAll(resp.Body)
		respStr := string(respBody)
		if !strings.Contains(respStr, "<Bucket>"+bucketName+"</Bucket>") {
			t.Errorf("Response should contain bucket name: %s", respStr)
		}
		if !strings.Contains(respStr, "<Key>"+objectKey+"</Key>") {
			t.Errorf("Response should contain key: %s", respStr)
		}

		// Cleanup
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	})

	t.Run("PostUploadWithStatus200", func(t *testing.T) {
		objectKey := "test-post-200.txt"
		content := "Hello with 200!"
		expiration := time.Now().Add(1 * time.Hour)

		conditions := []interface{}{
			map[string]string{"key": objectKey},
			map[string]string{"success_action_status": "200"},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, objectKey, expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}
		fields["success_action_status"] = "200"

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200, got %d: %s", resp.StatusCode, string(respBody))
		}

		// Cleanup
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	})

	t.Run("PostUploadWithFilenameSubstitution", func(t *testing.T) {
		content := "Hello with filename substitution!"
		expiration := time.Now().Add(1 * time.Hour)

		conditions := []interface{}{
			[]interface{}{"starts-with", "$key", "uploads/"},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, "uploads/${filename}", expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 204, got %d: %s", resp.StatusCode, string(respBody))
		}

		// Verify the object was created with substituted filename
		expectedKey := "uploads/upload.txt"
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(expectedKey),
		})
		if err != nil {
			t.Fatalf("Failed to get uploaded object with key %q: %v", expectedKey, err)
		}
		defer getOut.Body.Close()

		data, _ := io.ReadAll(getOut.Body)
		if string(data) != content {
			t.Errorf("Expected content %q, got %q", content, string(data))
		}

		// Cleanup
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(expectedKey),
		})
	})

	t.Run("PostUploadWrongSignature", func(t *testing.T) {
		objectKey := "test-post-wrong-sig.txt"
		content := "Should not be uploaded"
		expiration := time.Now().Add(1 * time.Hour)

		conditions := []interface{}{
			map[string]string{"key": objectKey},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, objectKey, expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}

		// Tamper with the signature
		fields["X-Amz-Signature"] = "0000000000000000000000000000000000000000000000000000000000000000"

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("Expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("PostUploadExpiredPolicy", func(t *testing.T) {
		objectKey := "test-post-expired.txt"
		content := "Should not be uploaded"
		// Policy expired in the past
		expiration := time.Now().Add(-1 * time.Hour)

		conditions := []interface{}{
			map[string]string{"key": objectKey},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, objectKey, expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("Expected 403, got %d", resp.StatusCode)
		}
	})

	t.Run("PostUploadWithMetadata", func(t *testing.T) {
		objectKey := "test-post-meta.txt"
		content := "Hello with metadata!"
		expiration := time.Now().Add(1 * time.Hour)

		conditions := []interface{}{
			map[string]string{"key": objectKey},
			[]interface{}{"starts-with", "$x-amz-meta-custom", ""},
			[]interface{}{"starts-with", "$Content-Type", ""},
		}

		fields, err := createPresignedPostFields(accessKey, secretKey, "us-east-1", bucketName, objectKey, expiration, conditions)
		if err != nil {
			t.Fatalf("Failed to create presigned POST fields: %v", err)
		}
		fields["x-amz-meta-custom"] = "my-metadata-value"
		fields["Content-Type"] = "text/plain"

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 204, got %d: %s", resp.StatusCode, string(respBody))
		}

		// Verify the metadata was stored
		headOut, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to head object: %v", err)
		}

		if headOut.Metadata["custom"] != "my-metadata-value" {
			t.Errorf("Expected metadata 'custom'='my-metadata-value', got %q", headOut.Metadata["custom"])
		}

		// Cleanup
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	})
	t.Run("PostUploadWithLowercaseFieldNames", func(t *testing.T) {
		// Ruby SDK uses lowercase field names: x-amz-credential, x-amz-algorithm, etc.
		objectKey := "test-post-lowercase.txt"
		content := "Hello from Ruby SDK style!"
		expiration := time.Now().Add(1 * time.Hour)

		now := time.Now().UTC()
		dateStr := now.Format("20060102")
		dateTimeStr := now.Format("20060102T150405Z")
		credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", accessKey, dateStr, "us-east-1")

		conditions := []interface{}{
			map[string]string{"bucket": bucketName},
			map[string]string{"key": objectKey},
			[]interface{}{"content-length-range", 1, 3145728},
			map[string]string{"x-amz-credential": credential},
			map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256"},
			map[string]string{"x-amz-date": dateTimeStr},
		}

		policy := map[string]interface{}{
			"expiration": expiration.UTC().Format("2006-01-02T15:04:05Z"),
			"conditions": conditions,
		}

		policyJSON, _ := json.Marshal(policy)
		policyBase64 := base64.StdEncoding.EncodeToString(policyJSON)

		signingKey := auth.CalculateSigningKey(secretKey, dateStr, "us-east-1", "s3")
		sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(policyBase64)))

		// Use lowercase field names like the Ruby SDK does
		fields := map[string]string{
			"key":              objectKey,
			"policy":           policyBase64,
			"x-amz-algorithm":  "AWS4-HMAC-SHA256",
			"x-amz-credential": credential,
			"x-amz-date":       dateTimeStr,
			"x-amz-signature":  sig,
		}

		body, contentType := buildMultipartForm(fields, content)

		resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
		if err != nil {
			t.Fatalf("Failed to POST object: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			respBody, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 204, got %d: %s", resp.StatusCode, string(respBody))
		}

		// Verify the object was created
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get uploaded object: %v", err)
		}
		defer getOut.Body.Close()

		data, _ := io.ReadAll(getOut.Body)
		if string(data) != content {
			t.Errorf("Expected content %q, got %q", content, string(data))
		}

		// Cleanup
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	})
}

func TestPostObjectWithoutAuth(t *testing.T) {
	// Test POST upload without authentication (no authenticator configured)
	tmpDir, err := os.MkdirTemp("", "s3d-post-noauth-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	s3Handler := server.NewS3Handler(store, server.WithRegion("us-east-1"))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	srv := &http.Server{Handler: s3Handler}
	go srv.Serve(listener)
	defer srv.Shutdown(context.Background())

	time.Sleep(100 * time.Millisecond)

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
		t.Fatalf("Failed to create config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "post-noauth-bucket"

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	objectKey := "noauth-post.txt"
	content := "No auth POST upload"

	// Build a simple form upload without policy
	fields := map[string]string{
		"key": objectKey,
	}
	body, contentType := buildMultipartForm(fields, content)

	resp, err := http.Post("http://"+addr+"/"+bucketName, contentType, body)
	if err != nil {
		t.Fatalf("Failed to POST object: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 204, got %d: %s", resp.StatusCode, string(respBody))
	}

	// Verify
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("Failed to get uploaded object: %v", err)
	}
	defer getOut.Body.Close()

	data, _ := io.ReadAll(getOut.Body)
	if string(data) != content {
		t.Errorf("Expected content %q, got %q", content, string(data))
	}

	// Cleanup
	client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
}
