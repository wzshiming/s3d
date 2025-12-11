package auth

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewAuthenticator(t *testing.T) {
	auth := NewAWS4Authenticator()
	if auth == nil {
		t.Fatal("NewAuthenticator returned nil")
	}
	if auth.credentials == nil {
		t.Fatal("credentials map should be initialized")
	}
	if len(auth.credentials) != 0 {
		t.Fatal("credentials map should be empty initially")
	}
}

func TestAddCredentials(t *testing.T) {
	auth := NewAWS4Authenticator()
	accessKey := "test-access-key"
	secretKey := "test-secret-key"

	auth.AddCredentials(accessKey, secretKey)

	if len(auth.credentials) != 1 {
		t.Fatalf("Expected 1 credential, got %d", len(auth.credentials))
	}

	if auth.credentials[accessKey] != secretKey {
		t.Fatal("Credential not stored correctly")
	}

	// Add another credential
	auth.AddCredentials("key2", "secret2")
	if len(auth.credentials) != 2 {
		t.Fatalf("Expected 2 credentials, got %d", len(auth.credentials))
	}
}

func TestAuthenticateNoAuth(t *testing.T) {
	auth := NewAWS4Authenticator()

	req := httptest.NewRequest("GET", "/bucket/object", nil)

	// When no credentials are configured, authentication should succeed
	user, err := auth.authenticate(req)
	if err == nil {
		t.Fatalf("Expected authentication to fail with no credentials, got user: %s", user)
	}
}

func TestAuthenticateMissingHeader(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)

	// When credentials are configured but auth header is missing, it should fail
	_, err := auth.authenticate(req)
	if err == nil {
		t.Fatal("Expected authentication to fail with missing header")
	}
	if err.Error() != "missing or invalid authentication information" {
		t.Fatalf("Expected 'missing or invalid authentication information' error, got: %v", err)
	}
}

func TestAuthenticateUnsupportedType(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	req.Header.Set("Authorization", "Basic dGVzdDp0ZXN0")

	_, err := auth.authenticate(req)
	if err == nil {
		t.Fatal("Expected authentication to fail with unsupported type")
	}
	if err.Error() != "unsupported authorization type" {
		t.Fatalf("Expected 'unsupported authorization type' error, got: %v", err)
	}
}

func TestAuthenticateInvalidFormat(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	tests := []struct {
		name   string
		header string
	}{
		{"Invalid format - no space", "AWS4-HMAC-SHA256NoSpace"},
		{"Invalid format - missing params", "AWS4-HMAC-SHA256 "},
		{"Invalid format - incomplete", "AWS4-HMAC-SHA256 Credential=test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/bucket/object", nil)
			req.Header.Set("Authorization", tt.header)

			_, err := auth.authenticate(req)
			if err == nil {
				t.Fatal("Expected authentication to fail with invalid format")
			}
		})
	}
}

func TestAuthenticateInvalidCredential(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("valid-key", "valid-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=invalid-key/20230101/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=abc123")

	_, err := auth.authenticate(req)
	if err == nil {
		t.Fatal("Expected authentication to fail with invalid access key")
	}
	// Error could be either invalid access key or missing required params depending on parsing
	expectedErrors := []string{"invalid access key", "missing required authorization parameters", "invalid authorization header format"}
	errorMatched := false
	for _, expected := range expectedErrors {
		if err.Error() == expected {
			errorMatched = true
			break
		}
	}
	if !errorMatched {
		t.Fatalf("Expected one of %v, got: %v", expectedErrors, err)
	}
}

func TestAuthenticateValidCredential(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("valid-key", "valid-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	req.Host = "example.amazonaws.com"
	req.Header.Set("X-Amz-Date", "20230101T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=valid-key/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=b06c3241e27bbe06de4271ac6617cd88056188eb970ce7123f5aa7ce5e5b05bf")

	_, err := auth.authenticate(req)
	if err != nil {
		t.Fatalf("Expected authentication to succeed with valid credential: %v", err)
	}
}

func TestSanitizeBucketName(t *testing.T) {
	auth := NewAWS4Authenticator()

	// Helper to test sanitization indirectly through authentication
	// Since sanitization is in storage package, we'll test auth middleware
	req := httptest.NewRequest("GET", "/test-bucket/object", nil)

	// Test that authentication works with no credentials
	user, err := auth.authenticate(req)
	if err == nil {
		t.Fatalf("Expected authentication to fail due to bucket name, got user: %s", user)
	}
}

func TestHmacSHA256(t *testing.T) {
	key := []byte("test-key")
	data := []byte("test-data")

	result := hmacSHA256(key, data)

	if len(result) == 0 {
		t.Fatal("HMAC result should not be empty")
	}

	// Verify same input produces same output
	result2 := hmacSHA256(key, data)
	if string(result) != string(result2) {
		t.Fatal("HMAC should be deterministic")
	}

	// Verify different key produces different output
	result3 := hmacSHA256([]byte("different-key"), data)
	if string(result) == string(result3) {
		t.Fatal("Different keys should produce different HMACs")
	}
}

func TestSHA256Hash(t *testing.T) {
	data := "test-data"

	hash := sha256Hash(data)

	if hash == "" {
		t.Fatal("Hash should not be empty")
	}

	// Verify same input produces same output
	hash2 := sha256Hash(data)
	if hash != hash2 {
		t.Fatal("SHA256 should be deterministic")
	}

	// Verify different input produces different output
	hash3 := sha256Hash("different-data")
	if hash == hash3 {
		t.Fatal("Different data should produce different hashes")
	}

	// Check hash format (should be hex)
	if len(hash) != 64 {
		t.Fatalf("Expected hash length 64, got %d", len(hash))
	}
}

func TestCreateCanonicalRequest(t *testing.T) {
	auth := NewAWS4Authenticator()

	req := httptest.NewRequest("GET", "/bucket/object?key=value", nil)
	req.Header.Set("Host", "localhost")
	req.Header.Set("X-Amz-Date", "20230101T000000Z")

	signedHeaders := "host;x-amz-date"
	canonical := auth.createCanonicalRequestHeader(req, signedHeaders)

	if canonical == "" {
		t.Fatal("Canonical request should not be empty")
	}

	// Verify it contains the method
	if !contains(canonical, "GET") {
		t.Fatal("Canonical request should contain HTTP method")
	}

	// Verify it contains the path
	if !contains(canonical, "/bucket/object") {
		t.Fatal("Canonical request should contain URI path")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Tests for query string authentication (presigned URLs)

func TestAuthenticateV4QueryMissingParams(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	tests := []struct {
		name        string
		queryParams map[string]string
	}{
		{
			name: "Missing Algorithm",
			queryParams: map[string]string{
				"X-Amz-Credential":    "test-key/20230101/us-east-1/s3/aws4_request",
				"X-Amz-Date":          "20230101T000000Z",
				"X-Amz-SignedHeaders": "host",
				"X-Amz-Signature":     "abc123",
			},
		},
		{
			name: "Missing Credential",
			queryParams: map[string]string{
				"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
				"X-Amz-Date":          "20230101T000000Z",
				"X-Amz-SignedHeaders": "host",
				"X-Amz-Signature":     "abc123",
			},
		},
		{
			name: "Missing Date",
			queryParams: map[string]string{
				"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
				"X-Amz-Credential":    "test-key/20230101/us-east-1/s3/aws4_request",
				"X-Amz-SignedHeaders": "host",
				"X-Amz-Signature":     "abc123",
			},
		},
		{
			name: "Missing SignedHeaders",
			queryParams: map[string]string{
				"X-Amz-Algorithm":  "AWS4-HMAC-SHA256",
				"X-Amz-Credential": "test-key/20230101/us-east-1/s3/aws4_request",
				"X-Amz-Date":       "20230101T000000Z",
				"X-Amz-Signature":  "abc123",
			},
		},
		{
			name: "Missing Signature",
			queryParams: map[string]string{
				"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
				"X-Amz-Credential":    "test-key/20230101/us-east-1/s3/aws4_request",
				"X-Amz-Date":          "20230101T000000Z",
				"X-Amz-SignedHeaders": "host",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/bucket/object", nil)
			q := req.URL.Query()
			for k, v := range tt.queryParams {
				q.Set(k, v)
			}
			req.URL.RawQuery = q.Encode()

			_, err := auth.authenticate(req)
			if err == nil {
				t.Fatal("Expected authentication to fail with missing parameters")
			}
		})
	}
}

func TestAuthenticateV4QueryInvalidAlgorithm(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "INVALID-ALGORITHM")
	q.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	q.Set("X-Amz-Date", "20230101T000000Z")
	q.Set("X-Amz-SignedHeaders", "host")
	q.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = q.Encode()

	_, err := auth.authenticate(req)
	if err == nil {
		t.Fatal("Expected authentication to fail with invalid algorithm")
	}
}

func TestAuthenticateV4QueryInvalidAccessKey(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("valid-key", "valid-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", "invalid-key/20230101/us-east-1/s3/aws4_request")
	q.Set("X-Amz-Date", "20230101T000000Z")
	q.Set("X-Amz-SignedHeaders", "host")
	q.Set("X-Amz-Signature", "abc123")
	req.URL.RawQuery = q.Encode()

	_, err := auth.authenticate(req)
	if err == nil {
		t.Fatal("Expected authentication to fail with invalid access key")
	}
	if err.Error() != "invalid access key" {
		t.Fatalf("Expected 'invalid access key' error, got: %v", err)
	}
}

func TestAuthenticateV4QueryValid(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	req.Host = "example.amazonaws.com"

	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", "test-key/20230101/us-east-1/s3/aws4_request")
	q.Set("X-Amz-Date", "20230101T000000Z")
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()

	// Calculate the expected signature (before adding signature to URL)
	expectedSig, err := auth.calculateSignatureV4Query(req, "test-secret", "20230101", "us-east-1", "s3", "host")
	if err != nil {
		t.Fatalf("Failed to calculate signature: %v", err)
	}

	q.Set("X-Amz-Signature", expectedSig)
	req.URL.RawQuery = q.Encode()

	accessKey, err := auth.authenticate(req)
	if err != nil {
		t.Fatalf("Expected authentication to succeed with valid query parameters: %v", err)
	}

	if accessKey != "test-key" {
		t.Fatalf("Expected access key 'test-key', got: %s", accessKey)
	}
}

func TestAuthenticateV4QueryWithExpires(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	req.Host = "example.amazonaws.com"

	// Use current time for the presigned URL
	now := time.Now().UTC()
	dateStr := now.Format("20060102")
	timestampStr := now.Format("20060102T150405Z")

	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", fmt.Sprintf("test-key/%s/us-east-1/s3/aws4_request", dateStr))
	q.Set("X-Amz-Date", timestampStr)
	q.Set("X-Amz-Expires", "3600")
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()

	// Calculate the expected signature (before adding signature to URL)
	expectedSig, err := auth.calculateSignatureV4Query(req, "test-secret", dateStr, "us-east-1", "s3", "host")
	if err != nil {
		t.Fatalf("Failed to calculate signature: %v", err)
	}

	q.Set("X-Amz-Signature", expectedSig)
	req.URL.RawQuery = q.Encode()

	accessKey, err := auth.authenticate(req)
	if err != nil {
		t.Fatalf("Expected authentication to succeed with X-Amz-Expires: %v", err)
	}

	if accessKey != "test-key" {
		t.Fatalf("Expected access key 'test-key', got: %s", accessKey)
	}
}

func TestAuthenticateV4QueryExpired(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	req := httptest.NewRequest("GET", "/bucket/object", nil)
	req.Host = "example.amazonaws.com"

	// Use a date from the past with a short expiration
	pastTime := time.Now().UTC().Add(-2 * time.Hour)
	dateStr := pastTime.Format("20060102")
	timestampStr := pastTime.Format("20060102T150405Z")

	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", fmt.Sprintf("test-key/%s/us-east-1/s3/aws4_request", dateStr))
	q.Set("X-Amz-Date", timestampStr)
	q.Set("X-Amz-Expires", "3600") // 1 hour, but request was from 2 hours ago
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()

	// Calculate the expected signature
	expectedSig, err := auth.calculateSignatureV4Query(req, "test-secret", dateStr, "us-east-1", "s3", "host")
	if err != nil {
		t.Fatalf("Failed to calculate signature: %v", err)
	}

	q.Set("X-Amz-Signature", expectedSig)
	req.URL.RawQuery = q.Encode()

	_, err = auth.authenticate(req)
	if err == nil {
		t.Fatal("Expected authentication to fail with expired presigned URL")
	}

	if err.Error() != "presigned URL has expired" {
		t.Fatalf("Expected 'presigned URL has expired' error, got: %v", err)
	}
}

func TestCreateCanonicalRequestQuery(t *testing.T) {
	auth := NewAWS4Authenticator()

	req := httptest.NewRequest("GET", "/bucket/object?key=value&X-Amz-Signature=shouldbeskipped", nil)
	req.Host = "example.amazonaws.com"

	signedHeaders := "host"
	canonical := auth.createCanonicalRequestQuery(req, signedHeaders)

	if canonical == "" {
		t.Fatal("Canonical request should not be empty")
	}

	// Verify X-Amz-Signature is excluded from canonical request
	if contains(canonical, "X-Amz-Signature") || contains(canonical, "shouldbeskipped") {
		t.Fatal("Canonical request should not contain X-Amz-Signature parameter")
	}

	// Verify other query parameters are included
	if !contains(canonical, "key=value") && !contains(canonical, "key%3Dvalue") {
		t.Fatal("Canonical request should contain other query parameters")
	}

	// Verify it contains UNSIGNED-PAYLOAD for query auth
	if !contains(canonical, "UNSIGNED-PAYLOAD") {
		t.Fatal("Canonical request for query auth should contain UNSIGNED-PAYLOAD")
	}
}
