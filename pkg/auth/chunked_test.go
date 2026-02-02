package auth

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseChunkHeader(t *testing.T) {
	tests := []struct {
		name          string
		header        string
		expectedSize  int64
		expectedSig   string
		expectedError bool
	}{
		{
			name:         "valid header",
			header:       "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
			expectedSize: 0x10000,
			expectedSig:  "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
		},
		{
			name:         "zero size final chunk",
			header:       "0;chunk-signature=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expectedSize: 0,
			expectedSig:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:          "missing semicolon",
			header:        "10000chunk-signature=abc",
			expectedError: true,
		},
		{
			name:          "invalid size",
			header:        "xyz;chunk-signature=abc",
			expectedError: true,
		},
		{
			name:          "missing signature prefix",
			header:        "10000;abc=def",
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, sig, err := parseChunkHeader(tt.header)
			if tt.expectedError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if size != tt.expectedSize {
				t.Errorf("size: expected %d, got %d", tt.expectedSize, size)
			}
			if sig != tt.expectedSig {
				t.Errorf("sig: expected %s, got %s", tt.expectedSig, sig)
			}
		})
	}
}

func TestCalculateSigningKey(t *testing.T) {
	// Test with known values from AWS documentation
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	date := "20130524"
	region := "us-east-1"
	service := "s3"

	signingKey := CalculateSigningKey(secretKey, date, region, service)

	// The signing key should be non-empty
	if len(signingKey) == 0 {
		t.Error("signing key should not be empty")
	}

	// Same inputs should produce same output
	signingKey2 := CalculateSigningKey(secretKey, date, region, service)
	if !bytes.Equal(signingKey, signingKey2) {
		t.Error("signing key should be deterministic")
	}

	// Different inputs should produce different output
	signingKey3 := CalculateSigningKey(secretKey, "20130525", region, service)
	if bytes.Equal(signingKey, signingKey3) {
		t.Error("different dates should produce different signing keys")
	}
}

func TestIsChunkedUpload(t *testing.T) {
	tests := []struct {
		name            string
		contentSha256   string
		contentEncoding string
		expected        bool
	}{
		{
			name:          "streaming payload hash",
			contentSha256: StreamingPayloadHash,
			expected:      true,
		},
		{
			name:            "aws-chunked encoding",
			contentEncoding: AWS4ChunkedEncoding,
			expected:        true,
		},
		{
			name:            "aws-chunked with gzip",
			contentEncoding: "aws-chunked,gzip",
			expected:        true,
		},
		{
			name:     "regular upload",
			expected: false,
		},
		{
			name:          "unsigned payload",
			contentSha256: "UNSIGNED-PAYLOAD",
			expected:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "/bucket/key", nil)
			if tt.contentSha256 != "" {
				req.Header.Set("X-Amz-Content-Sha256", tt.contentSha256)
			}
			if tt.contentEncoding != "" {
				req.Header.Set("Content-Encoding", tt.contentEncoding)
			}
			result := IsChunkedUpload(req)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetDecodedContentLength(t *testing.T) {
	tests := []struct {
		name          string
		headerValue   string
		expectedValue int64
	}{
		{
			name:          "valid length",
			headerValue:   "66560",
			expectedValue: 66560,
		},
		{
			name:          "zero length",
			headerValue:   "0",
			expectedValue: 0,
		},
		{
			name:          "no header",
			headerValue:   "",
			expectedValue: -1,
		},
		{
			name:          "invalid value",
			headerValue:   "abc",
			expectedValue: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "/bucket/key", nil)
			if tt.headerValue != "" {
				req.Header.Set("X-Amz-Decoded-Content-Length", tt.headerValue)
			}
			result := GetDecodedContentLength(req)
			if result != tt.expectedValue {
				t.Errorf("expected %d, got %d", tt.expectedValue, result)
			}
		})
	}
}

func TestChunkedReaderSimple(t *testing.T) {
	// Create a test signing key
	secretKey := "test-secret"
	date := "20230101"
	region := "us-east-1"
	service := "s3"
	signingKey := CalculateSigningKey(secretKey, date, region, service)

	credScope := date + "/" + region + "/" + service + "/aws4_request"
	timestamp := "20230101T000000Z"

	// Create some test data
	testData := []byte("Hello, World!")
	chunkHash := sha256Hash(string(testData))

	// Calculate the chunk signature manually
	seedSignature := "seed-signature"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		timestamp,
		credScope,
		seedSignature,
		emptyStringSHA256,
		chunkHash,
	}, "\n")
	chunkSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Calculate final chunk signature
	finalStringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		timestamp,
		credScope,
		chunkSig,
		emptyStringSHA256,
		emptyStringSHA256, // empty final chunk
	}, "\n")
	finalSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(finalStringToSign)))

	// Build the chunked body
	var buf bytes.Buffer
	// First chunk
	buf.WriteString("d;chunk-signature=" + chunkSig + "\r\n")
	buf.Write(testData)
	buf.WriteString("\r\n")
	// Final chunk
	buf.WriteString("0;chunk-signature=" + finalSig + "\r\n")

	// Create chunked reader
	reader := NewChunkedReader(&buf, signingKey, credScope, timestamp, seedSignature)

	// Read all data
	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.Equal(result, testData) {
		t.Errorf("expected %q, got %q", string(testData), string(result))
	}
}

func TestChunkedReaderMultipleChunks(t *testing.T) {
	// Create a test signing key
	secretKey := "test-secret"
	date := "20230101"
	region := "us-east-1"
	service := "s3"
	signingKey := CalculateSigningKey(secretKey, date, region, service)

	credScope := date + "/" + region + "/" + service + "/aws4_request"
	timestamp := "20230101T000000Z"

	// Create some test data chunks
	chunk1 := []byte("First chunk data")
	chunk2 := []byte("Second chunk")

	// Calculate signatures
	seedSignature := "seed-signature"

	// Chunk 1 signature
	chunk1Hash := sha256Hash(string(chunk1))
	chunk1StringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		timestamp,
		credScope,
		seedSignature,
		emptyStringSHA256,
		chunk1Hash,
	}, "\n")
	chunk1Sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(chunk1StringToSign)))

	// Chunk 2 signature (uses chunk 1 signature as previous)
	chunk2Hash := sha256Hash(string(chunk2))
	chunk2StringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		timestamp,
		credScope,
		chunk1Sig,
		emptyStringSHA256,
		chunk2Hash,
	}, "\n")
	chunk2Sig := hex.EncodeToString(hmacSHA256(signingKey, []byte(chunk2StringToSign)))

	// Final chunk signature
	finalStringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		timestamp,
		credScope,
		chunk2Sig,
		emptyStringSHA256,
		emptyStringSHA256,
	}, "\n")
	finalSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(finalStringToSign)))

	// Build the chunked body
	var buf bytes.Buffer
	// Chunk 1 (hex 10 = decimal 16)
	buf.WriteString("10;chunk-signature=" + chunk1Sig + "\r\n")
	buf.Write(chunk1)
	buf.WriteString("\r\n")
	// Chunk 2 (hex c = decimal 12)
	buf.WriteString("c;chunk-signature=" + chunk2Sig + "\r\n")
	buf.Write(chunk2)
	buf.WriteString("\r\n")
	// Final chunk
	buf.WriteString("0;chunk-signature=" + finalSig + "\r\n")

	// Create chunked reader
	reader := NewChunkedReader(&buf, signingKey, credScope, timestamp, seedSignature)

	// Read all data
	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedData := append(chunk1, chunk2...)
	if !bytes.Equal(result, expectedData) {
		t.Errorf("expected %q, got %q", string(expectedData), string(result))
	}
}

func TestChunkedReaderInvalidSignature(t *testing.T) {
	secretKey := "test-secret"
	date := "20230101"
	region := "us-east-1"
	service := "s3"
	signingKey := CalculateSigningKey(secretKey, date, region, service)

	credScope := date + "/" + region + "/" + service + "/aws4_request"
	timestamp := "20230101T000000Z"
	seedSignature := "seed-signature"

	// Create a chunk with an invalid signature
	var buf bytes.Buffer
	buf.WriteString("5;chunk-signature=invalidsignature12345678901234567890123456789012\r\n")
	buf.WriteString("Hello\r\n")
	buf.WriteString("0;chunk-signature=invalidsignature12345678901234567890123456789012\r\n")

	reader := NewChunkedReader(&buf, signingKey, credScope, timestamp, seedSignature)

	_, err := io.ReadAll(reader)
	if err == nil {
		t.Error("expected error for invalid signature")
	}
}

func TestChunkedReaderInvalidFormat(t *testing.T) {
	secretKey := "test-secret"
	date := "20230101"
	region := "us-east-1"
	service := "s3"
	signingKey := CalculateSigningKey(secretKey, date, region, service)

	credScope := date + "/" + region + "/" + service + "/aws4_request"
	timestamp := "20230101T000000Z"
	seedSignature := "seed-signature"

	// Test invalid chunk format (missing semicolon)
	var buf bytes.Buffer
	buf.WriteString("5chunk-signature=abc\r\n")

	reader := NewChunkedReader(&buf, signingKey, credScope, timestamp, seedSignature)

	_, err := io.ReadAll(reader)
	if err == nil {
		t.Error("expected error for invalid format")
	}
}

func TestWrapChunkedRequest(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("test-key", "test-secret")

	t.Run("non-chunked request returns original", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/key", nil)
		result, err := auth.WrapChunkedRequest(req)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != req {
			t.Error("expected original request to be returned")
		}
	})

	t.Run("chunked request missing auth header", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/key", nil)
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		_, err := auth.WrapChunkedRequest(req)
		if err == nil {
			t.Error("expected error for missing auth header")
		}
	})

	t.Run("chunked request with invalid credentials", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/bucket/key", nil)
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Date", "20230101T000000Z")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=invalid-key/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123")
		_, err := auth.WrapChunkedRequest(req)
		if err == nil {
			t.Error("expected error for invalid credentials")
		}
	})

	t.Run("chunked request with valid auth wraps body", func(t *testing.T) {
		// Create chunked body
		secretKey := "test-secret"
		date := "20230101"
		region := "us-east-1"
		service := "s3"
		signingKey := CalculateSigningKey(secretKey, date, region, service)

		credScope := date + "/" + region + "/" + service + "/aws4_request"
		timestamp := "20230101T000000Z"
		seedSignature := "abc123"

		testData := []byte("Test data!")
		chunkHash := sha256Hash(string(testData))

		// Calculate chunk signature
		chunkStringToSign := strings.Join([]string{
			"AWS4-HMAC-SHA256-PAYLOAD",
			timestamp,
			credScope,
			seedSignature,
			emptyStringSHA256,
			chunkHash,
		}, "\n")
		chunkSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(chunkStringToSign)))

		// Calculate final chunk signature
		finalStringToSign := strings.Join([]string{
			"AWS4-HMAC-SHA256-PAYLOAD",
			timestamp,
			credScope,
			chunkSig,
			emptyStringSHA256,
			emptyStringSHA256,
		}, "\n")
		finalSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(finalStringToSign)))

		var buf bytes.Buffer
		buf.WriteString("a;chunk-signature=" + chunkSig + "\r\n")
		buf.Write(testData)
		buf.WriteString("\r\n")
		buf.WriteString("0;chunk-signature=" + finalSig + "\r\n")

		req := httptest.NewRequest("PUT", "/bucket/key", &buf)
		req.Header.Set("X-Amz-Content-Sha256", StreamingPayloadHash)
		req.Header.Set("X-Amz-Date", timestamp)
		req.Header.Set("X-Amz-Decoded-Content-Length", "10")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test-key/20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature="+seedSignature)

		result, err := auth.WrapChunkedRequest(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify the body can be read
		body, err := io.ReadAll(result.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}

		if !bytes.Equal(body, testData) {
			t.Errorf("expected %q, got %q", string(testData), string(body))
		}

		// Verify content length was updated
		if result.ContentLength != 10 {
			t.Errorf("expected content length 10, got %d", result.ContentLength)
		}
	})
}

func TestGetSecretKey(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("key1", "secret1")
	auth.AddCredentials("key2", "secret2")

	if auth.GetSecretKey("key1") != "secret1" {
		t.Error("expected secret1")
	}
	if auth.GetSecretKey("key2") != "secret2" {
		t.Error("expected secret2")
	}
	if auth.GetSecretKey("key3") != "" {
		t.Error("expected empty string for unknown key")
	}
}
