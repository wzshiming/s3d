package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func TestAWSURIEncode(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		encodeSlash bool
		expected    string
	}{
		{"plain", "abc-123._~", true, "abc-123._~"},
		{"space uses %20 not +", "foo bar", true, "foo%20bar"},
		{"plus is encoded", "foo+1", true, "foo%2B1"},
		{"percent is encoded", "bar%", true, "bar%25"},
		{"unicode", "テスト", true, "%E3%83%86%E3%82%B9%E3%83%88"},
		{"slash kept for path", "foo/bar", false, "foo/bar"},
		{"slash encoded for query", "text/plain", true, "text%2Fplain"},
		{"equals and ampersand", "a=b&c", true, "a%3Db%26c"},
		{"tilde not encoded", "~user", true, "~user"},
		{"whitespace chars", "a\tb\nc", true, "a%09b%0Ac"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := awsURIEncode(tt.input, tt.encodeSlash); got != tt.expected {
				t.Fatalf("awsURIEncode(%q, %v) = %q, want %q", tt.input, tt.encodeSlash, got, tt.expected)
			}
		})
	}
}

func TestCanonicalHeaderValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain", "value", "value"},
		{"trim", "  value  ", "value"},
		{"collapse spaces", "a   b  c", "a b c"},
		{"tabs collapsed", "a\t\tb", "a b"},
		{"base64 value untouched", "pO3upElrwuteZALVf2cakSpVaqtLYGvZpo3VYUuqgE8=", "pO3upElrwuteZALVf2cakSpVaqtLYGvZpo3VYUuqgE8="},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canonicalHeaderValue(tt.input); got != tt.expected {
				t.Fatalf("canonicalHeaderValue(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestCreateCanonicalRequestSpecialCharacters(t *testing.T) {
	auth := NewAWS4Authenticator()

	tests := []struct {
		name     string
		method   string
		target   string
		headers  map[string]string
		signed   string
		expected string
	}{
		{
			name:   "key with plus and slash",
			method: "GET",
			target: "http://example.com/bucket/foo%2B1/bar",
			signed: "host",
			expected: strings.Join([]string{
				"GET",
				"/bucket/foo%2B1/bar",
				"",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "key with trailing percent",
			method: "GET",
			target: "http://example.com/bucket/foo/bar%25",
			signed: "host",
			expected: strings.Join([]string{
				"GET",
				"/bucket/foo/bar%25",
				"",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "unicode key",
			method: "GET",
			target: "http://example.com/bucket/%E3%83%86%E3%82%B9%E3%83%88",
			signed: "host",
			expected: strings.Join([]string{
				"GET",
				"/bucket/%E3%83%86%E3%82%B9%E3%83%88",
				"",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "query params without values",
			method: "POST",
			target: "http://example.com/bucket/key?select&select-type=2",
			signed: "host",
			expected: strings.Join([]string{
				"POST",
				"/bucket/key",
				"select=&select-type=2",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "response params encoded with %20 not plus",
			method: "GET",
			target: "http://example.com/bucket/key?response-content-type=text%2Fplain&response-content-disposition=attachment%3B%20filename%3D%22foo%22",
			signed: "host",
			expected: strings.Join([]string{
				"GET",
				"/bucket/key",
				"response-content-disposition=attachment%3B%20filename%3D%22foo%22&response-content-type=text%2Fplain",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "repeated query params sorted by value",
			method: "GET",
			target: "http://example.com/bucket?tag=b&tag=a",
			signed: "host",
			expected: strings.Join([]string{
				"GET",
				"/bucket",
				"tag=a&tag=b",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "whitespace delimiter in query",
			method: "GET",
			target: "http://example.com/bucket?delimiter=%20&prefix=%09",
			signed: "host",
			expected: strings.Join([]string{
				"GET",
				"/bucket",
				"delimiter=%20&prefix=%09",
				"host:example.com\n",
				"host",
				"UNSIGNED-PAYLOAD",
			}, "\n"),
		},
		{
			name:   "sse-c headers with base64 values",
			method: "PUT",
			target: "http://example.com/bucket/key",
			headers: map[string]string{
				"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
				"X-Amz-Server-Side-Encryption-Customer-Key":       "pO3upElrwuteZALVf2cakSpVaqtLYGvZpo3VYUuqgE8=",
				"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   "DWygnHRtgiJ77HCm+1rvHw==",
				"X-Amz-Content-Sha256":                            "abc123",
			},
			signed: "host;x-amz-server-side-encryption-customer-algorithm;x-amz-server-side-encryption-customer-key;x-amz-server-side-encryption-customer-key-md5",
			expected: strings.Join([]string{
				"PUT",
				"/bucket/key",
				"",
				"host:example.com\n" +
					"x-amz-server-side-encryption-customer-algorithm:AES256\n" +
					"x-amz-server-side-encryption-customer-key:pO3upElrwuteZALVf2cakSpVaqtLYGvZpo3VYUuqgE8=\n" +
					"x-amz-server-side-encryption-customer-key-md5:DWygnHRtgiJ77HCm+1rvHw==\n",
				"host;x-amz-server-side-encryption-customer-algorithm;x-amz-server-side-encryption-customer-key;x-amz-server-side-encryption-customer-key-md5",
				"abc123",
			}, "\n"),
		},
		{
			name:   "copy source condition headers",
			method: "PUT",
			target: "http://example.com/bucket/dst",
			headers: map[string]string{
				"X-Amz-Copy-Source":          "/bucket/src",
				"X-Amz-Copy-Source-If-Match": "\"e58c67ba9dcb5c46a67dbbb8160a4e12\"",
				"X-Amz-Content-Sha256":       "abc123",
			},
			signed: "host;x-amz-copy-source;x-amz-copy-source-if-match",
			expected: strings.Join([]string{
				"PUT",
				"/bucket/dst",
				"",
				"host:example.com\n" +
					"x-amz-copy-source:/bucket/src\n" +
					"x-amz-copy-source-if-match:\"e58c67ba9dcb5c46a67dbbb8160a4e12\"\n",
				"host;x-amz-copy-source;x-amz-copy-source-if-match",
				"abc123",
			}, "\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.target, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			got := auth.createCanonicalRequestHeader(req, tt.signed)
			if got != tt.expected {
				t.Fatalf("canonical request mismatch\ngot:\n%s\nwant:\n%s", got, tt.expected)
			}
		})
	}
}

// TestSigV4EndToEndSpecialCharacters signs requests with the official AWS SDK
// v4 signer and validates them against the authenticator through a real HTTP
// server, covering keys/query/headers with special characters.
func TestSigV4EndToEndSpecialCharacters(t *testing.T) {
	const (
		accessKey = "test-access-key"
		secretKey = "test-secret-key"
		region    = "us-east-1"
	)

	authenticator := NewAWS4Authenticator()
	authenticator.AddCredentials(accessKey, secretKey)

	server := httptest.NewServer(authenticator.AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	defer server.Close()

	tests := []struct {
		name    string
		method  string
		path    string // already URI-encoded path + optional query
		body    string
		headers map[string]string
	}{
		{name: "plus in key", method: "PUT", path: "/bucket/foo%2B1/bar", body: "data"},
		{name: "space in key", method: "PUT", path: "/bucket/foo%20bar", body: "data"},
		{name: "trailing percent in key", method: "PUT", path: "/bucket/foo/bar%25", body: "data"},
		{name: "unicode key", method: "PUT", path: "/bucket/%E3%83%86%E3%82%B9%E3%83%88/%E3%82%AA%E3%83%96%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88", body: "data"},
		{name: "special chars key", method: "PUT", path: "/bucket/%20%21%22%23%24%25%26%27%28%29", body: "data"},
		{name: "select with valueless param", method: "POST", path: "/bucket/key?select&select-type=2", body: "<xml/>"},
		{name: "response headers query", method: "GET", path: "/bucket/key?response-content-type=text%2Fplain&response-content-disposition=attachment%3B%20filename%3D%22foo%22&response-cache-control=no-cache"},
		{name: "whitespace delimiter", method: "GET", path: "/bucket?delimiter=%20&prefix=%09&encoding-type=url"},
		{name: "repeated query params", method: "GET", path: "/bucket?tag=b&tag=a"},
		{
			name: "sse-c headers", method: "PUT", path: "/bucket/sse-key", body: "data",
			headers: map[string]string{
				"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
				"X-Amz-Server-Side-Encryption-Customer-Key":       "pO3upElrwuteZALVf2cakSpVaqtLYGvZpo3VYUuqgE8=",
				"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   "DWygnHRtgiJ77HCm+1rvHw==",
			},
		},
		{
			name: "copy source conditions", method: "PUT", path: "/bucket/dst",
			headers: map[string]string{
				"X-Amz-Copy-Source":               "/bucket/src%20file",
				"X-Amz-Copy-Source-If-Match":      "\"e58c67ba9dcb5c46a67dbbb8160a4e12\"",
				"X-Amz-Copy-Source-If-None-Match": "\"0000000000000000000000000000dead\"",
			},
		},
	}

	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		// S3 clients sign the URI path as-is without double-escaping
		o.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			req, err := http.NewRequest(tt.method, server.URL+tt.path, body)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			sum := sha256.Sum256([]byte(tt.body))
			payloadHash := hex.EncodeToString(sum[:])
			req.Header.Set("X-Amz-Content-Sha256", payloadHash)

			if err := signer.SignHTTP(context.Background(), creds, req, payloadHash, "s3", region, time.Now().UTC()); err != nil {
				t.Fatalf("failed to sign request: %v", err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				respBody, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, respBody)
			}
		})
	}
}

// TestSigV4PresignedEndToEndSpecialCharacters validates presigned (query auth)
// URLs with special characters against the authenticator.
func TestSigV4PresignedEndToEndSpecialCharacters(t *testing.T) {
	const (
		accessKey = "test-access-key"
		secretKey = "test-secret-key"
		region    = "us-east-1"
	)

	authenticator := NewAWS4Authenticator()
	authenticator.AddCredentials(accessKey, secretKey)

	server := httptest.NewServer(authenticator.AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	defer server.Close()

	tests := []struct {
		name string
		path string // already URI-encoded path + optional query
	}{
		{name: "space in key", path: "/bucket/foo%20bar"},
		{name: "plus in key", path: "/bucket/foo%2B1/bar"},
		{name: "unicode key", path: "/bucket/%E3%83%86%E3%82%B9%E3%83%88"},
		{name: "response params", path: "/bucket/key?response-content-type=text%2Fplain"},
	}

	signer := v4.NewSigner(func(o *v4.SignerOptions) {
		// S3 clients sign the URI path as-is without double-escaping
		o.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", server.URL+tt.path, nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			signedURI, _, err := signer.PresignHTTP(context.Background(), creds, req, "UNSIGNED-PAYLOAD", "s3", region, time.Now().UTC())
			if err != nil {
				t.Fatalf("failed to presign request: %v", err)
			}

			presignedReq, err := http.NewRequest("GET", signedURI, nil)
			if err != nil {
				t.Fatalf("failed to create presigned request: %v", err)
			}

			resp, err := http.DefaultClient.Do(presignedReq)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				respBody, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, respBody)
			}
		})
	}
}
