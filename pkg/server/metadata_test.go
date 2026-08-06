package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestObjectMetadata(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-metadata-bucket"
	objectKey := "test-object-with-metadata.txt"
	objectContent := "Hello, metadata world!"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	t.Run("PutObjectWithMetadata", func(t *testing.T) {
		// Put object with metadata
		metadata := map[string]string{
			"author":      "test-user",
			"version":     "1.0",
			"description": "test description",
		}

		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			Body:     strings.NewReader(objectContent),
			Metadata: metadata,
		})
		if err != nil {
			t.Fatalf("PutObject with metadata failed: %v", err)
		}

		// Get object and verify metadata
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		// Verify content
		body, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}
		if string(body) != objectContent {
			t.Errorf("Object content mismatch: got %q, want %q", string(body), objectContent)
		}

		// Verify metadata
		if output.Metadata == nil {
			t.Fatal("Expected metadata to be present, but it was nil")
		}

		for key, expectedValue := range metadata {
			actualValue, ok := output.Metadata[key]
			if !ok {
				t.Errorf("Metadata key %q not found in response", key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("Metadata[%q] = %q, want %q", key, actualValue, expectedValue)
			}
		}
	})

	t.Run("UpdateMetadataWithSameContent", func(t *testing.T) {
		// Put the same content again with different metadata
		newMetadata := map[string]string{
			"author":  "updated-user",
			"version": "2.0",
			"status":  "updated",
		}

		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			Body:     strings.NewReader(objectContent),
			Metadata: newMetadata,
		})
		if err != nil {
			t.Fatalf("PutObject with updated metadata failed: %v", err)
		}

		// Get object and verify metadata was updated
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		// Verify metadata was updated
		for key, expectedValue := range newMetadata {
			actualValue, ok := output.Metadata[key]
			if !ok {
				t.Errorf("Updated metadata key %q not found in response", key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("Updated Metadata[%q] = %q, want %q", key, actualValue, expectedValue)
			}
		}

		// Verify old metadata key is no longer present
		if _, ok := output.Metadata["description"]; ok {
			t.Error("Old metadata key 'description' should not be present after update")
		}
	})

	t.Run("HeadObjectWithMetadata", func(t *testing.T) {
		// HEAD request should also return metadata
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		// Verify metadata (should have the updated metadata from previous test)
		if output.Metadata == nil {
			t.Fatal("Expected metadata to be present in HEAD response, but it was nil")
		}

		expectedMetadata := map[string]string{
			"author":  "updated-user",
			"version": "2.0",
			"status":  "updated",
		}

		for key, expectedValue := range expectedMetadata {
			actualValue, ok := output.Metadata[key]
			if !ok {
				t.Errorf("Metadata key %q not found in HEAD response", key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("HEAD Metadata[%q] = %q, want %q", key, actualValue, expectedValue)
			}
		}
	})

	t.Run("CopyObjectPreservesMetadata", func(t *testing.T) {
		// Copy the object
		copyKey := "copied-object.txt"
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(copyKey),
			CopySource: aws.String(bucketName + "/" + objectKey),
		})
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}
		defer ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(copyKey)})

		// Get copied object and verify metadata is preserved
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(copyKey),
		})
		if err != nil {
			t.Fatalf("GetObject on copied object failed: %v", err)
		}
		defer output.Body.Close()

		// Verify metadata was preserved
		expectedMetadata := map[string]string{
			"author":  "updated-user",
			"version": "2.0",
			"status":  "updated",
		}

		for key, expectedValue := range expectedMetadata {
			actualValue, ok := output.Metadata[key]
			if !ok {
				t.Errorf("Metadata key %q not found in copied object", key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("Copied object Metadata[%q] = %q, want %q", key, actualValue, expectedValue)
			}
		}
	})

	t.Run("PutObjectWithoutMetadata", func(t *testing.T) {
		// Put object without metadata
		noMetaKey := "no-metadata-object.txt"
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(noMetaKey),
			Body:   strings.NewReader("content without metadata"),
		})
		if err != nil {
			t.Fatalf("PutObject without metadata failed: %v", err)
		}
		defer ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(noMetaKey)})

		// Get object and verify metadata is empty or nil
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(noMetaKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		// Metadata should be empty or nil
		if output.Metadata != nil && len(output.Metadata) > 0 {
			t.Errorf("Expected no metadata, but got: %v", output.Metadata)
		}
	})
}

func TestMultipartUploadMetadata(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multipart-metadata-bucket"
	objectKey := "multipart-object-with-metadata.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	t.Run("MultipartUploadWithMetadata", func(t *testing.T) {
		// Initiate multipart upload with metadata
		metadata := map[string]string{
			"multipart-test": "true",
			"parts-count":    "2",
		}

		createOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			Metadata: metadata,
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}
		uploadID := createOutput.UploadId

		// Upload parts
		part1Data := bytes.Repeat([]byte("a"), 5*1024*1024) // 5MB
		part1Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(part1Data),
		})
		if err != nil {
			t.Fatalf("UploadPart 1 failed: %v", err)
		}

		part2Data := bytes.Repeat([]byte("b"), 5*1024*1024) // 5MB
		part2Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(part2Data),
		})
		if err != nil {
			t.Fatalf("UploadPart 2 failed: %v", err)
		}

		// Complete multipart upload
		_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       part1Output.ETag,
						PartNumber: aws.Int32(1),
					},
					{
						ETag:       part2Output.ETag,
						PartNumber: aws.Int32(2),
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}
		defer ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)})

		// Get object and verify metadata
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		// Verify metadata
		if output.Metadata == nil {
			t.Fatal("Expected metadata to be present, but it was nil")
		}

		for key, expectedValue := range metadata {
			actualValue, ok := output.Metadata[key]
			if !ok {
				t.Errorf("Metadata key %q not found in multipart object", key)
				continue
			}
			if actualValue != expectedValue {
				t.Errorf("Multipart Metadata[%q] = %q, want %q", key, actualValue, expectedValue)
			}
		}
	})
}

func TestObjectStandardMetadataHeaders(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-standard-metadata-bucket"
	objectKey := "standard-metadata-object.txt"

	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	expires := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)

	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(objectKey),
		Body:            strings.NewReader("standard metadata content"),
		ContentEncoding: aws.String("gzip"),
		ContentLanguage: aws.String("en-US"),
		Expires:         aws.Time(expires),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	defer ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)})

	t.Run("HeadObjectReturnsStandardHeaders", func(t *testing.T) {
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		if aws.ToString(output.ContentEncoding) != "gzip" {
			t.Errorf("ContentEncoding = %q, want %q", aws.ToString(output.ContentEncoding), "gzip")
		}
		if aws.ToString(output.ContentLanguage) != "en-US" {
			t.Errorf("ContentLanguage = %q, want %q", aws.ToString(output.ContentLanguage), "en-US")
		}
		if output.ExpiresString == nil || *output.ExpiresString == "" {
			t.Error("Expected Expires header to be present")
		}
	})

	t.Run("CopyObjectAppliesStorageClass", func(t *testing.T) {
		copyKey := "storage-class-copy.txt"
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(copyKey),
			CopySource:   aws.String(bucketName + "/" + objectKey),
			StorageClass: types.StorageClassReducedRedundancy,
		})
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}
		defer ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(copyKey)})

		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(copyKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}
		if output.StorageClass != types.StorageClassReducedRedundancy {
			t.Errorf("StorageClass = %q, want %q", output.StorageClass, types.StorageClassReducedRedundancy)
		}

		// Original metadata should still be preserved on copy
		if aws.ToString(output.ContentLanguage) != "en-US" {
			t.Errorf("ContentLanguage = %q, want %q", aws.ToString(output.ContentLanguage), "en-US")
		}

		// Listing should report the applied storage class
		listOutput, err := ts.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(copyKey),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}
		if len(listOutput.Contents) != 1 {
			t.Fatalf("Expected 1 object in listing, got %d", len(listOutput.Contents))
		}
		if listOutput.Contents[0].StorageClass != types.ObjectStorageClassReducedRedundancy {
			t.Errorf("Listing StorageClass = %q, want %q", listOutput.Contents[0].StorageClass, types.ObjectStorageClassReducedRedundancy)
		}
	})

	t.Run("EmptyMetadataValuePreserved", func(t *testing.T) {
		emptyMetaKey := "empty-metadata-object.txt"
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(emptyMetaKey),
			Body:     strings.NewReader("content"),
			Metadata: map[string]string{"meta1": ""},
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		defer ts.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucketName), Key: aws.String(emptyMetaKey)})

		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(emptyMetaKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}
		value, ok := output.Metadata["meta1"]
		if !ok {
			t.Fatal("Metadata key 'meta1' with empty value was not preserved")
		}
		if value != "" {
			t.Errorf("Metadata['meta1'] = %q, want empty string", value)
		}
	})
}

func TestStripAWSChunkedEncoding(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"aws-chunked", ""},
		{"aws-chunked,gzip", "gzip"},
		{"aws-chunked, gzip", "gzip"},
		{"gzip", "gzip"},
		{"gzip,aws-chunked", "gzip"},
	}
	for _, tt := range tests {
		if got := stripAWSChunkedEncoding(tt.input); got != tt.want {
			t.Errorf("stripAWSChunkedEncoding(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestXMLTimestampFormat(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-xml-timestamp-bucket"

	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	resp, err := http.Get("http://" + ts.listener.Addr().String() + "/")
	if err != nil {
		t.Fatalf("ListBuckets request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	re := regexp.MustCompile(`<CreationDate>([^<]+)</CreationDate>`)
	matches := re.FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		t.Fatalf("No CreationDate found in ListBuckets response: %s", string(body))
	}
	for _, m := range matches {
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", m[1]); err != nil {
			t.Errorf("CreationDate %q is not in ISO8601 millisecond format: %v", m[1], err)
		}
		if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`).MatchString(m[1]) {
			t.Errorf("CreationDate %q does not match expected format 2006-01-02T15:04:05.000Z", m[1])
		}
	}
}
