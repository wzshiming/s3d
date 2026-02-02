package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestChunkedUpload tests AWS S3 chunked upload format
// This tests the server's ability to handle chunked uploads that are typically
// used by AWS SDK when streaming data without a known content length
func TestChunkedUpload(t *testing.T) {
	bucketName := "chunked-upload-bucket"
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test: Simulate chunked upload with raw HTTP request
	t.Run("ChunkedUploadRawHTTP", func(t *testing.T) {
		objectKey := "chunked-object.txt"
		objectContent := "Hello, this is chunked upload content!"

		// Build chunked body: format is <hex-size>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n\r\n
		var chunkedBody bytes.Buffer
		// Write chunk 1 (size in hex)
		chunkSize := fmt.Sprintf("%x", len(objectContent))
		chunkedBody.WriteString(chunkSize + ";chunk-signature=dummy123\r\n")
		chunkedBody.WriteString(objectContent + "\r\n")
		// Write final chunk
		chunkedBody.WriteString("0;chunk-signature=dummyfinal\r\n\r\n")

		// Create HTTP request
		url := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, objectKey)
		req, err := http.NewRequestWithContext(context.Background(), "PUT", url, &chunkedBody)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		// Set headers to indicate chunked upload
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", len(objectContent)))

		// Send request
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 OK, got %d: %s", resp.StatusCode, string(body))
		}

		// Verify object was created correctly
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Content mismatch: got %q, want %q", string(data), objectContent)
		}
	})

	// Test: Multiple chunks
	t.Run("MultipleChunks", func(t *testing.T) {
		objectKey := "multi-chunk-object.txt"
		chunk1 := "First chunk data. "
		chunk2 := "Second chunk data. "
		chunk3 := "Third chunk data."
		expectedContent := chunk1 + chunk2 + chunk3

		var chunkedBody bytes.Buffer
		// Chunk 1
		chunkedBody.WriteString(fmt.Sprintf("%x;chunk-signature=sig1\r\n%s\r\n", len(chunk1), chunk1))
		// Chunk 2
		chunkedBody.WriteString(fmt.Sprintf("%x;chunk-signature=sig2\r\n%s\r\n", len(chunk2), chunk2))
		// Chunk 3
		chunkedBody.WriteString(fmt.Sprintf("%x;chunk-signature=sig3\r\n%s\r\n", len(chunk3), chunk3))
		// Final chunk
		chunkedBody.WriteString("0;chunk-signature=sigfinal\r\n\r\n")

		url := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, objectKey)
		req, err := http.NewRequestWithContext(context.Background(), "PUT", url, &chunkedBody)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", len(expectedContent)))

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 OK, got %d: %s", resp.StatusCode, string(body))
		}

		// Verify content
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		if string(data) != expectedContent {
			t.Errorf("Content mismatch: got %q, want %q", string(data), expectedContent)
		}
	})

	// Test: Large chunked upload
	t.Run("LargeChunkedUpload", func(t *testing.T) {
		objectKey := "large-chunked-object.bin"
		// Create 100KB of data split into 10KB chunks
		chunkSize := 10 * 1024
		numChunks := 10
		totalContent := strings.Repeat("A", chunkSize*numChunks)

		var chunkedBody bytes.Buffer
		for i := 0; i < numChunks; i++ {
			chunkData := totalContent[i*chunkSize : (i+1)*chunkSize]
			chunkedBody.WriteString(fmt.Sprintf("%x;chunk-signature=sig%d\r\n%s\r\n", len(chunkData), i, chunkData))
		}
		chunkedBody.WriteString("0;chunk-signature=sigfinal\r\n\r\n")

		url := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, objectKey)
		req, err := http.NewRequestWithContext(context.Background(), "PUT", url, &chunkedBody)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", len(totalContent)))

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 OK, got %d: %s", resp.StatusCode, string(body))
		}

		// Verify content size
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		if len(data) != len(totalContent) {
			t.Errorf("Size mismatch: got %d bytes, want %d bytes", len(data), len(totalContent))
		}

		if string(data) != totalContent {
			t.Errorf("Content mismatch")
		}
	})

	// Test: Empty chunked upload
	t.Run("EmptyChunkedUpload", func(t *testing.T) {
		objectKey := "empty-chunked-object.txt"

		var chunkedBody bytes.Buffer
		// Only final chunk with size 0
		chunkedBody.WriteString("0;chunk-signature=sigfinal\r\n\r\n")

		url := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, objectKey)
		req, err := http.NewRequestWithContext(context.Background(), "PUT", url, &chunkedBody)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		req.Header.Set("x-amz-decoded-content-length", "0")

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 OK, got %d: %s", resp.StatusCode, string(body))
		}

		// Verify empty content
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		if len(data) != 0 {
			t.Errorf("Expected empty content, got %d bytes", len(data))
		}
	})

	// Test: Chunked upload with trailing headers
	t.Run("ChunkedUploadWithTrailingHeaders", func(t *testing.T) {
		objectKey := "trailing-headers-object.txt"
		objectContent := "Content with trailing headers"

		var chunkedBody bytes.Buffer
		chunkSize := fmt.Sprintf("%x", len(objectContent))
		chunkedBody.WriteString(chunkSize + ";chunk-signature=sig\r\n")
		chunkedBody.WriteString(objectContent + "\r\n")
		// Final chunk with trailing headers
		chunkedBody.WriteString("0;chunk-signature=sigfinal\r\n")
		chunkedBody.WriteString("x-amz-checksum-sha256:abc123\r\n")
		chunkedBody.WriteString("\r\n")

		url := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, objectKey)
		req, err := http.NewRequestWithContext(context.Background(), "PUT", url, &chunkedBody)
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}

		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
		req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", len(objectContent)))

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("Expected 200 OK, got %d: %s", resp.StatusCode, string(body))
		}

		// Verify content
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Content mismatch: got %q, want %q", string(data), objectContent)
		}
	})

	// Test: Regular (non-chunked) upload still works
	t.Run("RegularUploadStillWorks", func(t *testing.T) {
		objectKey := "regular-object.txt"
		objectContent := "Regular upload content"

		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}

		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to get object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Content mismatch: got %q, want %q", string(data), objectContent)
		}
	})
}
