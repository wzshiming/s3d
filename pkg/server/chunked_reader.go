// Package server provides HTTP handling for S3-compatible operations.
//
// This file implements the chunked reader for AWS S3 chunked uploads.
// AWS S3 chunked uploads use a specific format where each chunk includes
// size and signature information, described at:
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
package server

import (
	"bufio"
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
)

// ChunkedReader reads AWS S3 chunked upload format.
// Each chunk has the format:
// <hex-size>;chunk-signature=<signature>\r\n
// <data>\r\n
//
// The final chunk has size 0:
// 0;chunk-signature=<signature>\r\n\r\n
type ChunkedReader struct {
	reader    *bufio.Reader
	remaining int // bytes remaining in current chunk
	done      bool
	err       error
}

// NewChunkedReader creates a new ChunkedReader wrapping the given io.Reader.
func NewChunkedReader(r io.Reader) *ChunkedReader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &ChunkedReader{
		reader: br,
	}
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

	if err != nil && err != io.EOF {
		c.err = err
		return n, err
	}

	// If we've finished this chunk, consume the trailing \r\n
	if c.remaining == 0 {
		if err := c.consumeCRLF(); err != nil {
			c.err = err
			return n, err
		}
	}

	return n, nil
}

// readChunkHeader reads and parses the chunk header.
// Format: <hex-size>[;chunk-signature=<signature>]\r\n
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

	// Parse size from the header
	// Format can be: "size" or "size;chunk-signature=sig" or "size;extension=value"
	parts := strings.SplitN(line, ";", 2)
	sizeStr := strings.TrimSpace(parts[0])

	if sizeStr == "" {
		return ErrInvalidChunkFormat
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
		// Final chunk - consume any trailing content
		// There may be trailing headers after the final chunk
		c.done = true
		// Consume the trailing \r\n after the 0-size chunk
		c.consumeTrailingHeaders()
		return nil
	}

	c.remaining = int(size)
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
// This is indicated by the x-amz-content-sha256 header containing
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD or similar streaming indicators.
func IsChunkedUpload(contentSha256 string) bool {
	return strings.HasPrefix(contentSha256, "STREAMING-")
}
