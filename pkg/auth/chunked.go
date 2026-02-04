// Package auth implements AWS Signature V4 streaming authentication for chunked uploads.
//
// This file implements the AWS Signature Version 4 streaming payload signing
// as described in: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
//
// Chunked uploads use the following format:
// - Content-Encoding: aws-chunked
// - x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD
// - Each chunk: hex-size;chunk-signature=signature\r\ndata\r\n
package auth

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// streamingPayloadHash is the payload hash value for streaming uploads
const streamingPayloadHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

// aws4ChunkedEncoding is the content encoding value for AWS chunked uploads
const aws4ChunkedEncoding = "aws-chunked"

// ChunkSignaturePrefix is the prefix for chunk signatures
const chunkSignaturePrefix = "chunk-signature="

// emptyStringSHA256 is the SHA256 hash of an empty string
const emptyStringSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// ErrInvalidChunkFormat is returned when a chunk has an invalid format
var ErrInvalidChunkFormat = errors.New("invalid chunk format")

// ErrChunkSignatureMismatch is returned when a chunk signature doesn't match
var ErrChunkSignatureMismatch = errors.New("chunk signature mismatch")

// ChunkedReader reads and validates AWS SigV4 chunked encoded data.
// It verifies each chunk's signature as it reads.
type ChunkedReader struct {
	reader        *bufio.Reader
	signingKey    []byte
	credScope     string
	timestamp     string
	prevSignature string
	chunkData     []byte
	chunkOffset   int
	eof           bool
	err           error
}

// NewChunkedReader creates a new ChunkedReader for validating chunked uploads.
// - r: the request body reader
// - signingKey: the signing key derived from credentials
// - credScope: the credential scope (date/region/service/aws4_request)
// - timestamp: the X-Amz-Date value
// - seedSignature: the signature from the Authorization header
func NewChunkedReader(r io.Reader, signingKey []byte, credScope, timestamp, seedSignature string) io.Reader {
	return &ChunkedReader{
		reader:        bufio.NewReader(r),
		signingKey:    signingKey,
		credScope:     credScope,
		timestamp:     timestamp,
		prevSignature: seedSignature,
	}
}

// Read implements io.Reader
func (c *ChunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.eof {
		return 0, io.EOF
	}

	// If we have remaining data from the current chunk, return it
	if c.chunkOffset < len(c.chunkData) {
		n := copy(p, c.chunkData[c.chunkOffset:])
		c.chunkOffset += n
		return n, nil
	}

	// Read the next chunk
	if err := c.readNextChunk(); err != nil {
		c.err = err
		if errors.Is(err, io.EOF) {
			c.eof = true
			return 0, io.EOF
		}
		return 0, err
	}

	// Return data from the new chunk
	n := copy(p, c.chunkData[c.chunkOffset:])
	c.chunkOffset += n
	return n, nil
}

// readNextChunk reads and validates the next chunk from the stream
func (c *ChunkedReader) readNextChunk() error {
	// Read the chunk header line: hex-size;chunk-signature=signature
	headerLine, err := c.reader.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) && headerLine == "" {
			return io.EOF
		}
		return fmt.Errorf("failed to read chunk header: %w", err)
	}

	// Remove trailing \r\n
	headerLine = strings.TrimSuffix(headerLine, "\n")
	headerLine = strings.TrimSuffix(headerLine, "\r")

	// Parse chunk header
	chunkSize, signature, err := parseChunkHeader(headerLine)
	if err != nil {
		return err
	}

	// A chunk size of 0 indicates the final chunk
	if chunkSize == 0 {
		// Validate final chunk signature
		expectedSig := c.calculateChunkSignature(nil)
		if signature != expectedSig {
			return fmt.Errorf("%w: expected %s, got %s", ErrChunkSignatureMismatch, expectedSig, signature)
		}
		c.prevSignature = signature
		return io.EOF
	}

	// Read the chunk data
	chunkData := make([]byte, chunkSize)
	_, err = io.ReadFull(c.reader, chunkData)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}

	// Read the trailing \r\n after chunk data
	trailer := make([]byte, 2)
	_, err = io.ReadFull(c.reader, trailer)
	if err != nil {
		return fmt.Errorf("failed to read chunk trailer: %w", err)
	}
	if !bytes.Equal(trailer, []byte("\r\n")) {
		return ErrInvalidChunkFormat
	}

	// Validate chunk signature
	expectedSig := c.calculateChunkSignature(chunkData)
	if signature != expectedSig {
		return fmt.Errorf("%w: expected %s, got %s", ErrChunkSignatureMismatch, expectedSig, signature)
	}

	// Update state for next chunk
	c.prevSignature = signature
	c.chunkData = chunkData
	c.chunkOffset = 0

	return nil
}

// calculateChunkSignature calculates the signature for a chunk
// According to AWS docs, the string to sign for chunk signatures is:
// AWS4-HMAC-SHA256-PAYLOAD
// timestamp
// credential_scope
// previous_signature
// hash(empty_string) for chunk-extensions (we don't use extensions)
// hash(current_chunk_data)
func (c *ChunkedReader) calculateChunkSignature(chunkData []byte) string {
	var chunkHash string
	if len(chunkData) == 0 {
		chunkHash = emptyStringSHA256
	} else {
		chunkHash = sha256Hash(string(chunkData))
	}

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		c.timestamp,
		c.credScope,
		c.prevSignature,
		emptyStringSHA256, // hash of empty chunk extension
		chunkHash,
	}, "\n")

	signature := hmacSHA256(c.signingKey, []byte(stringToSign))
	return hex.EncodeToString(signature)
}

// parseChunkHeader parses a chunk header line
// Format: hex-size;chunk-signature=signature
func parseChunkHeader(header string) (size int64, signature string, err error) {
	parts := strings.SplitN(header, ";", 2)
	if len(parts) != 2 {
		return 0, "", ErrInvalidChunkFormat
	}

	// Parse size
	size, err = strconv.ParseInt(parts[0], 16, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid chunk size: %w", err)
	}

	// Parse signature
	sigPart := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(sigPart, chunkSignaturePrefix) {
		return 0, "", ErrInvalidChunkFormat
	}
	signature = strings.TrimPrefix(sigPart, chunkSignaturePrefix)

	return size, signature, nil
}

// IsChunkedUpload checks if the request is an AWS chunked upload
func IsChunkedUpload(r *http.Request) bool {
	contentSha256 := r.Header.Get("X-Amz-Content-Sha256")
	contentEncoding := r.Header.Get("Content-Encoding")

	return contentSha256 == streamingPayloadHash ||
		strings.Contains(contentEncoding, aws4ChunkedEncoding)
}

// getDecodedContentLength returns the decoded content length for chunked uploads.
// Returns -1 if not a chunked upload or if the header is not present.
func getDecodedContentLength(r *http.Request) int64 {
	decodedLen := r.Header.Get("X-Amz-Decoded-Content-Length")
	if decodedLen == "" {
		return -1
	}
	length, err := strconv.ParseInt(decodedLen, 10, 64)
	if err != nil {
		return -1
	}
	return length
}

// WrapChunkedRequest wraps the request body with a ChunkedReader for validation.
// Returns the original request if it's not a chunked upload.
// The seedSignature is extracted from the Authorization header.
func (a *AWS4Authenticator) WrapChunkedRequest(r *http.Request) (*http.Request, error) {
	if !IsChunkedUpload(r) {
		return r, nil
	}

	// Parse the authorization header to get the seed signature and credentials
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, NewAuthError("AccessDenied", "Missing authorization header")
	}

	// Extract signature from auth header
	authParams := strings.TrimPrefix(authHeader, "AWS4-HMAC-SHA256 ")
	params := make(map[string]string)
	for _, part := range strings.Split(authParams, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			params[kv[0]] = kv[1]
		}
	}

	seedSignature := params["Signature"]
	credential := params["Credential"]
	if seedSignature == "" || credential == "" {
		return nil, NewAuthError("InvalidArgument", "Missing signature or credential")
	}

	// Parse credential
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return nil, NewAuthError("InvalidArgument", "Invalid credential format")
	}

	accessKeyID := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]
	credScope := strings.Join(credParts[1:], "/")

	// Get secret key
	secretAccessKey, exists := a.credentials[accessKeyID]
	if !exists {
		return nil, NewAuthError("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records")
	}

	// Get timestamp
	timestamp := r.Header.Get("X-Amz-Date")
	if timestamp == "" {
		timestamp = r.Header.Get("Date")
	}

	// Calculate signing key
	signingKey := CalculateSigningKey(secretAccessKey, date, region, service)

	// Create chunked reader
	chunkedReader := NewChunkedReader(r.Body, signingKey, credScope, timestamp, seedSignature)

	// Create a new request with the wrapped body
	newReq := r.Clone(r.Context())
	newReq.Body = struct {
		io.Reader
		io.Closer
	}{
		Reader: chunkedReader,
		Closer: r.Body,
	}

	// Update Content-Length if x-amz-decoded-content-length is present
	decodedLen := getDecodedContentLength(r)
	if decodedLen >= 0 {
		newReq.ContentLength = decodedLen
		newReq.Header.Del("X-Amz-Decoded-Content-Length")
	}

	return newReq, nil
}
