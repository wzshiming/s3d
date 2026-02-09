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

// TestPresignedPost tests presigned POST uploads
func TestPresignedPost(t *testing.T) {
	// Setup test server with authentication
	tmpDir, err := os.MkdirTemp("", "s3d-presigned-post-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	authenticator := auth.NewAWS4Authenticator()
	accessKey := "test-access-key"
	secretKey := "test-secret-key"
	authenticator.AddCredentials(accessKey, secretKey)

	s3Handler := server.NewS3Handler(store,
		server.WithRegion("us-east-1"),
		server.WithAuthenticator(authenticator))

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

	// Create S3 client to set up bucket
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
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

	bucketName := "test-presigned-post-bucket"
	objectKey := "test-file.txt"
	fileContent := "Hello from presigned POST!"

	// Create bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	t.Run("PresignedPostUpload", func(t *testing.T) {
		// Create policy
		expiration := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
		const maxUploadSize = 10485760 // 10MB
		policy := map[string]interface{}{
			"expiration": expiration,
			"conditions": []interface{}{
				map[string]string{"bucket": bucketName},
				[]interface{}{"starts-with", "$key", ""},
				[]interface{}{"starts-with", "$Content-Type", ""},
				[]interface{}{"content-length-range", 0, maxUploadSize},
			},
		}

		policyJSON, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("Failed to marshal policy: %v", err)
		}

		policyBase64 := base64.StdEncoding.EncodeToString(policyJSON)

		// Calculate signature
		date := time.Now().UTC().Format("20060102")
		region := "us-east-1"
		service := "s3"

		// Derive signing key
		dateKey := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
		dateRegionKey := hmacSHA256(dateKey, []byte(region))
		dateRegionServiceKey := hmacSHA256(dateRegionKey, []byte(service))
		signingKey := hmacSHA256(dateRegionServiceKey, []byte("aws4_request"))

		// Sign the policy
		signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(policyBase64)))

		credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", accessKey, date, region, service)

		// Create multipart form
		var formBuffer bytes.Buffer
		writer := multipart.NewWriter(&formBuffer)

		// Add form fields
		writer.WriteField("key", objectKey)
		writer.WriteField("policy", policyBase64)
		writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		writer.WriteField("x-amz-credential", credential)
		writer.WriteField("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))
		writer.WriteField("x-amz-signature", signature)
		writer.WriteField("Content-Type", "text/plain")

		// Add file
		part, err := writer.CreateFormFile("file", "test-file.txt")
		if err != nil {
			t.Fatalf("Failed to create form file: %v", err)
		}
		io.WriteString(part, fileContent)

		writer.Close()

		// Send POST request
		url := fmt.Sprintf("http://%s/%s", addr, bucketName)
		req, err := http.NewRequest("POST", url, &formBuffer)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		httpClient := &http.Client{}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request: %v", err)
		}
		defer resp.Body.Close()

		// Check response
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected status 204 or 201, got %d: %s", resp.StatusCode, string(body))
		}

		// Verify object was created
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer getResp.Body.Close()

		body, err := io.ReadAll(getResp.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}

		if string(body) != fileContent {
			t.Errorf("Expected content %q, got %q", fileContent, string(body))
		}
	})

	t.Run("PresignedPostInvalidSignature", func(t *testing.T) {
		// Create policy
		expiration := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
		policy := map[string]interface{}{
			"expiration": expiration,
			"conditions": []interface{}{
				map[string]string{"bucket": bucketName},
				[]interface{}{"starts-with", "$key", ""},
			},
		}

		policyJSON, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("Failed to marshal policy: %v", err)
		}

		policyBase64 := base64.StdEncoding.EncodeToString(policyJSON)

		// Use wrong signature
		wrongSignature := "0000000000000000000000000000000000000000000000000000000000000000"

		date := time.Now().UTC().Format("20060102")
		credential := fmt.Sprintf("%s/%s/us-east-1/s3/aws4_request", accessKey, date)

		// Create multipart form
		var formBuffer bytes.Buffer
		writer := multipart.NewWriter(&formBuffer)

		writer.WriteField("key", "invalid-test.txt")
		writer.WriteField("policy", policyBase64)
		writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		writer.WriteField("x-amz-credential", credential)
		writer.WriteField("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))
		writer.WriteField("x-amz-signature", wrongSignature)

		part, err := writer.CreateFormFile("file", "test.txt")
		if err != nil {
			t.Fatalf("Failed to create form file: %v", err)
		}
		io.WriteString(part, "test content")

		writer.Close()

		// Send POST request
		url := fmt.Sprintf("http://%s/%s", addr, bucketName)
		req, err := http.NewRequest("POST", url, &formBuffer)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		httpClient := &http.Client{}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request: %v", err)
		}
		defer resp.Body.Close()

		// Should get 403 Forbidden
		if resp.StatusCode != http.StatusForbidden {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("Expected status 403, got %d: %s", resp.StatusCode, string(body))
			return
		}

		// Verify error response contains SignatureDoesNotMatch
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "SignatureDoesNotMatch") {
			t.Errorf("Expected SignatureDoesNotMatch error, got: %s", string(body))
		}
	})

	t.Run("PresignedPostExpiredPolicy", func(t *testing.T) {
		// Create policy with expiration in the past
		expiration := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
		policy := map[string]interface{}{
			"expiration": expiration,
			"conditions": []interface{}{
				map[string]string{"bucket": bucketName},
				[]interface{}{"starts-with", "$key", ""},
			},
		}

		policyJSON, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("Failed to marshal policy: %v", err)
		}

		policyBase64 := base64.StdEncoding.EncodeToString(policyJSON)

		// Calculate signature
		date := time.Now().UTC().Format("20060102")
		region := "us-east-1"
		service := "s3"

		dateKey := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
		dateRegionKey := hmacSHA256(dateKey, []byte(region))
		dateRegionServiceKey := hmacSHA256(dateRegionKey, []byte(service))
		signingKey := hmacSHA256(dateRegionServiceKey, []byte("aws4_request"))
		signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(policyBase64)))

		credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", accessKey, date, region, service)

		// Create multipart form
		var formBuffer bytes.Buffer
		writer := multipart.NewWriter(&formBuffer)

		writer.WriteField("key", "expired-test.txt")
		writer.WriteField("policy", policyBase64)
		writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		writer.WriteField("x-amz-credential", credential)
		writer.WriteField("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))
		writer.WriteField("x-amz-signature", signature)

		part, err := writer.CreateFormFile("file", "test.txt")
		if err != nil {
			t.Fatalf("Failed to create form file: %v", err)
		}
		io.WriteString(part, "test content")

		writer.Close()

		// Send POST request
		url := fmt.Sprintf("http://%s/%s", addr, bucketName)
		req, err := http.NewRequest("POST", url, &formBuffer)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		httpClient := &http.Client{}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request: %v", err)
		}
		defer resp.Body.Close()

		// Should get 403 Forbidden
		if resp.StatusCode != http.StatusForbidden {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("Expected status 403, got %d: %s", resp.StatusCode, string(body))
		}
	})
}

// hmacSHA256 calculates HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
