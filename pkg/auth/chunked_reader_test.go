package auth

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestChunkedReader_SingleChunk(t *testing.T) {
	// Test data: single chunk with 5 bytes of data
	// Format: <hex-size>;chunk-signature=<sig>\r\n<data>\r\n0;chunk-signature=<sig>\r\n\r\n
	input := "5;chunk-signature=abc123\r\nhello\r\n0;chunk-signature=xyz789\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "hello"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestChunkedReader_MultipleChunks(t *testing.T) {
	// Test data: multiple chunks
	input := "5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=end\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "hello world"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestChunkedReader_NoSignature(t *testing.T) {
	// Test chunked format without signature extension (standard HTTP chunked)
	input := "5\r\nhello\r\n0\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "hello"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestChunkedReader_LargeChunk(t *testing.T) {
	// Test with larger data
	largeData := strings.Repeat("a", 1000)
	// Format chunk size as hex: 1000 = 0x3e8
	input := "3e8;chunk-signature=sig\r\n" + largeData + "\r\n0;chunk-signature=end\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(data) != largeData {
		t.Errorf("got %d bytes, want %d bytes", len(data), len(largeData))
	}
}

func TestChunkedReader_EmptyData(t *testing.T) {
	// Test empty upload (just the final chunk)
	input := "0;chunk-signature=abc\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(data))
	}
}

func TestChunkedReader_SmallReads(t *testing.T) {
	// Test reading small amounts at a time
	input := "a;chunk-signature=sig\r\n0123456789\r\n0;chunk-signature=end\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	// Read 3 bytes at a time
	var result []byte
	buf := make([]byte, 3)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	expected := "0123456789"
	if string(result) != expected {
		t.Errorf("got %q, want %q", string(result), expected)
	}
}

func TestChunkedReader_WithTrailingHeaders(t *testing.T) {
	// Test with trailing headers after final chunk
	input := "5;chunk-signature=abc\r\nhello\r\n0;chunk-signature=end\r\nx-amz-checksum-sha256:abc123\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "hello"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestChunkedReader_UppercaseHex(t *testing.T) {
	// Test with uppercase hex digits
	input := "A;chunk-signature=sig\r\n0123456789\r\n0;chunk-signature=end\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "0123456789"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestChunkedReader_BinaryData(t *testing.T) {
	// Test with binary data
	binaryData := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD}
	var buf bytes.Buffer
	buf.WriteString("7;chunk-signature=sig\r\n")
	buf.Write(binaryData)
	buf.WriteString("\r\n0;chunk-signature=end\r\n\r\n")

	reader := NewChunkedReader(&buf)

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !bytes.Equal(data, binaryData) {
		t.Errorf("got %v, want %v", data, binaryData)
	}
}

func TestIsChunkedUpload(t *testing.T) {
	tests := []struct {
		name            string
		contentEncoding string
		contentSha256   string
		expected        bool
	}{
		{"streaming sha256 payload", "", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", true},
		{"streaming sha256 payload trailer", "", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", true},
		{"streaming unsigned trailer", "", "STREAMING-UNSIGNED-PAYLOAD-TRAILER", true},
		{"unsigned payload", "", "UNSIGNED-PAYLOAD", false},
		{"empty headers", "", "", false},
		{"regular sha256", "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", false},
		{"aws-chunked encoding", "aws-chunked", "", true},
		{"aws-chunked with gzip", "aws-chunked,gzip", "", true},
		{"gzip encoding only", "gzip", "", false},
		{"aws-chunked with streaming sha256", "aws-chunked", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsChunkedUpload(tc.contentEncoding, tc.contentSha256)
			if result != tc.expected {
				t.Errorf("IsChunkedUpload(%q, %q) = %v, want %v", tc.contentEncoding, tc.contentSha256, result, tc.expected)
			}
		})
	}
}

func TestChunkedReader_InvalidHex(t *testing.T) {
	input := "zz;chunk-signature=sig\r\ndata\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	_, err := io.ReadAll(reader)
	if err != ErrInvalidChunkFormat {
		t.Errorf("expected ErrInvalidChunkFormat, got %v", err)
	}
}

func TestChunkedReader_MultipleExtensions(t *testing.T) {
	// Test with multiple chunk extensions (not just signature)
	input := "5;chunk-signature=abc;other-ext=val\r\nhello\r\n0;chunk-signature=xyz\r\n\r\n"
	reader := NewChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "hello"
	if string(data) != expected {
		t.Errorf("got %q, want %q", string(data), expected)
	}
}

func TestGetSigningKey(t *testing.T) {
	auth := NewAWS4Authenticator()
	auth.AddCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	// Test getting signing key for valid credentials
	signingKey, err := auth.GetSigningKey("AKIAIOSFODNN7EXAMPLE", "20130524", "us-east-1", "s3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if signingKey == nil {
		t.Fatal("expected signing key, got nil")
	}

	// Test getting signing key for invalid credentials
	_, err = auth.GetSigningKey("INVALID_KEY", "20130524", "us-east-1", "s3")
	if err == nil {
		t.Fatal("expected error for invalid credentials")
	}
}
