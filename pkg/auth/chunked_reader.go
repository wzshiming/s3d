// Package auth implements AWS Signature V4 authentication for S3-compatible servers.
//
// This file implements the chunked reader for AWS S3 chunked uploads with
// optional signature validation. AWS S3 chunked uploads use a specific format
// where each chunk includes size and signature information, described at:
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
package auth

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
)

var (
	// ErrInvalidChunkFormat indicates a malformed chunk header
	ErrInvalidChunkFormat = errors.New("invalid chunk format")
	// ErrChunkTooLarge indicates a chunk size exceeds maximum
	ErrChunkTooLarge = errors.New("chunk size too large")
	// ErrInvalidChunkSignature indicates the chunk signature is invalid
	ErrInvalidChunkSignature = errors.New("invalid chunk signature")
)

// ChunkedReader reads AWS S3 chunked upload format.
// Each chunk has the format:
// <hex-size>;chunk-signature=<signature>\r\n
// <data>\r\n
//
// The final chunk has size 0:
// 0;chunk-signature=<signature>\r\n\r\n
type ChunkedReader struct {
	reader          *bufio.Reader
	remaining       int    // bytes remaining in current chunk
	done            bool
	err             error
	
	// Signature validation fields
	signingKey        []byte // derived signing key for signature validation
	region            string
	service           string
	timestamp         string
	prevSignature     string // previous chunk signature (seed signature for first chunk)
	validateSig       bool   // whether to validate chunk signatures
	currentChunkData  []byte // buffer for current chunk data (for signature validation)
	pendingSignature  string // signature to validate after chunk data is read
}

// ChunkedReaderOption configures a ChunkedReader
type ChunkedReaderOption func(*ChunkedReader)

// WithSignatureValidation enables chunk signature validation using the AWS4Authenticator.
// Parameters:
//   - auth: the AWS4Authenticator to use for signature validation
//   - accessKeyID: the access key ID from the request
//   - date: date string (YYYYMMDD) from the credential scope
//   - region: AWS region from the credential scope
//   - service: AWS service from the credential scope (typically "s3")
//   - timestamp: request timestamp (X-Amz-Date)
//   - seedSignature: the signature from the initial request authorization header
func WithSignatureValidation(auth *AWS4Authenticator, accessKeyID, date, region, service, timestamp, seedSignature string) ChunkedReaderOption {
	return func(c *ChunkedReader) {
		signingKey, err := auth.GetSigningKey(accessKeyID, date, region, service)
		if err != nil {
			// If we can't get the signing key, disable validation
			// The error will be caught during chunk processing
			return
		}
		c.signingKey = signingKey
		c.region = region
		c.service = service
		c.timestamp = timestamp
		c.prevSignature = seedSignature
		c.validateSig = true
	}
}

// NewChunkedReader creates a new ChunkedReader wrapping the given io.Reader.
func NewChunkedReader(r io.Reader, opts ...ChunkedReaderOption) *ChunkedReader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	c := &ChunkedReader{
		reader: br,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Read implements io.Reader for AWS S3 chunked format.
func (c *ChunkedReader) Read(p []byte) (n int, err error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.done {
		return 0, io.EOF
	}

	// If no remaining bytes in current chunk, read next chunk header
	if c.remaining == 0 {
		if err := c.readChunkHeader(); err != nil {
			c.err = err
			return 0, err
		}
		// Check if this was the final (zero-size) chunk
		if c.done {
			return 0, io.EOF
		}
	}

	// Read data from current chunk
	toRead := len(p)
	if toRead > c.remaining {
		toRead = c.remaining
	}

	n, err = c.reader.Read(p[:toRead])
	c.remaining -= n

	// If validating signatures, accumulate chunk data
	if c.validateSig && n > 0 {
		c.currentChunkData = append(c.currentChunkData, p[:n]...)
	}

	if err != nil && err != io.EOF {
		c.err = err
		return n, err
	}

	// If we've finished this chunk, consume the trailing \r\n and validate signature
	if c.remaining == 0 {
		// Validate signature for non-final chunks if enabled
		if c.validateSig && c.pendingSignature != "" {
			if err := c.validateChunkSignature(c.pendingSignature, c.currentChunkData); err != nil {
				c.err = err
				return n, err
			}
			c.pendingSignature = ""
		}
		if err := c.consumeCRLF(); err != nil {
			c.err = err
			return n, err
		}
		// Reset chunk data buffer for next chunk
		c.currentChunkData = nil
	}

	return n, nil
}

// readChunkHeader reads and parses the chunk header.
// Format: <hex-size>;chunk-signature=<signature>\r\n
func (c *ChunkedReader) readChunkHeader() error {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		if err == io.EOF && line == "" {
			c.done = true
			return io.EOF
		}
		return err
	}

	// Trim the trailing \r\n or just \n (for lenient parsing)
	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")

	// Parse size and extensions from the header
	// Format: "size;chunk-signature=sig" or "size;extension=value;..."
	parts := strings.SplitN(line, ";", 2)
	sizeStr := strings.TrimSpace(parts[0])

	if sizeStr == "" {
		return ErrInvalidChunkFormat
	}

	// Parse chunk-signature if present
	var chunkSignature string
	if len(parts) == 2 {
		extensions := parts[1]
		// Parse chunk-signature from extensions
		for _, ext := range strings.Split(extensions, ";") {
			ext = strings.TrimSpace(ext)
			if strings.HasPrefix(ext, "chunk-signature=") {
				chunkSignature = strings.TrimPrefix(ext, "chunk-signature=")
				break
			}
		}
	}

	size, err := strconv.ParseInt(sizeStr, 16, 64)
	if err != nil {
		return ErrInvalidChunkFormat
	}

	// Check for unreasonable chunk sizes.
	// AWS S3 does not specify a max chunk size, but typically uses 64KB-1MB chunks.
	// We allow up to 16 MB per chunk to be generous while preventing memory exhaustion.
	const maxChunkSize = 16 * 1024 * 1024
	if size > maxChunkSize {
		return ErrChunkTooLarge
	}

	if size == 0 {
		// Final chunk - validate signature if enabled
		if c.validateSig && chunkSignature != "" {
			if err := c.validateChunkSignature(chunkSignature, nil); err != nil {
				return err
			}
		}
		// Final chunk - consume any trailing content
		// There may be trailing headers after the final chunk
		c.done = true
		// Consume the trailing \r\n after the 0-size chunk
		c.consumeTrailingHeaders()
		return nil
	}

	// Store expected signature for validation after reading chunk data
	if c.validateSig && chunkSignature != "" {
		c.pendingSignature = chunkSignature
	}

	c.remaining = int(size)
	return nil
}

// validateChunkSignature validates the signature of a chunk
// According to AWS SigV4 streaming spec:
// string-to-sign = "AWS4-HMAC-SHA256-PAYLOAD" + "\n" +
//                  timestamp + "\n" +
//                  credential-scope + "\n" +
//                  previous-signature + "\n" +
//                  sha256("") + "\n" +  // hash of empty headers
//                  sha256(chunk-data)
func (c *ChunkedReader) validateChunkSignature(expectedSig string, chunkData []byte) error {
	if !c.validateSig || c.signingKey == nil {
		return nil
	}

	// Build credential scope
	dateStamp := c.timestamp[:8] // YYYYMMDD from YYYYMMDDTHHMMSSZ
	credentialScope := dateStamp + "/" + c.region + "/" + c.service + "/aws4_request"

	// Calculate hash of chunk data
	chunkHash := sha256.Sum256(chunkData)
	chunkHashHex := hex.EncodeToString(chunkHash[:])

	// Empty hash for the empty canonical headers
	emptyHash := sha256.Sum256([]byte(""))
	emptyHashHex := hex.EncodeToString(emptyHash[:])

	// Build string to sign
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		c.timestamp,
		credentialScope,
		c.prevSignature,
		emptyHashHex,
		chunkHashHex,
	}, "\n")

	// Calculate expected signature
	calculatedSig := hex.EncodeToString(hmacSHA256(c.signingKey, []byte(stringToSign)))

	if calculatedSig != expectedSig {
		return ErrInvalidChunkSignature
	}

	// Update previous signature for next chunk
	c.prevSignature = expectedSig

	return nil
}

// consumeCRLF consumes the trailing \r\n after chunk data.
func (c *ChunkedReader) consumeCRLF() error {
	// Read exactly 2 bytes for \r\n
	buf := make([]byte, 2)
	_, err := io.ReadFull(c.reader, buf)
	if err != nil {
		return err
	}
	// Validate it's actually \r\n (or just \n for lenient parsing)
	if buf[0] != '\r' && buf[0] != '\n' {
		return ErrInvalidChunkFormat
	}
	return nil
}

// consumeTrailingHeaders reads any trailing headers after the final chunk.
func (c *ChunkedReader) consumeTrailingHeaders() {
	// Read lines until we get an empty line or EOF
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return
		}
		// Empty line (just \r\n or \n) signals end of trailers
		if line == "\r\n" || line == "\n" {
			return
		}
	}
}

// IsChunkedUpload checks if the request uses AWS chunked upload encoding.
// AWS chunked uploads are indicated by:
// 1. Content-Encoding header containing "aws-chunked"
// 2. x-amz-content-sha256 header starting with "STREAMING-"
func IsChunkedUpload(contentEncoding, contentSha256 string) bool {
	// Check for aws-chunked content encoding
	if strings.Contains(contentEncoding, "aws-chunked") {
		return true
	}
	// Check for streaming content SHA256
	if strings.HasPrefix(contentSha256, "STREAMING-") {
		return true
	}
	return false
}

// GetSigningKey derives the signing key for chunk signature validation.
// This can be used by handlers that need to create ChunkedReader with signature validation.
func (a *AWS4Authenticator) GetSigningKey(accessKeyID, date, region, service string) ([]byte, error) {
	secretAccessKey, exists := a.credentials[accessKeyID]
	if !exists {
		return nil, NewAuthError("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records")
	}

	dateKey := hmacSHA256([]byte("AWS4"+secretAccessKey), []byte(date))
	dateRegionKey := hmacSHA256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSHA256(dateRegionKey, []byte(service))
	signingKey := hmacSHA256(dateRegionServiceKey, []byte("aws4_request"))

	return signingKey, nil
}
