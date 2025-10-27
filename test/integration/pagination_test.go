package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestObjectPagination tests pagination for object listing operations
func TestObjectPagination(t *testing.T) {
	bucketName := "test-object-pagination"

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Create multiple objects
	numObjects := 10
	for i := 0; i < numObjects; i++ {
		key := fmt.Sprintf("object-%03d.txt", i)
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(fmt.Sprintf("content-%d", i)),
		})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", key, err)
		}
	}

	// Test ListObjectsV2 pagination
	t.Run("ListObjectsV2_Pagination", func(t *testing.T) {
		maxKeys := int32(3)
		var allKeys []string

		// First page
		output, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(maxKeys),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if len(output.Contents) > int(maxKeys) {
			t.Errorf("Expected at most %d objects, got %d", maxKeys, len(output.Contents))
		}

		for _, obj := range output.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		// Check IsTruncated flag
		if !*output.IsTruncated {
			t.Error("Expected IsTruncated=true for first page")
		}

		if output.NextContinuationToken == nil {
			t.Error("Expected NextContinuationToken to be set")
		}

		// Fetch remaining pages
		for output.IsTruncated != nil && *output.IsTruncated {
			output, err = ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(bucketName),
				MaxKeys:           aws.Int32(maxKeys),
				ContinuationToken: output.NextContinuationToken,
			})
			if err != nil {
				t.Fatalf("ListObjectsV2 continuation failed: %v", err)
			}

			for _, obj := range output.Contents {
				allKeys = append(allKeys, *obj.Key)
			}
		}

		// Verify we got all objects
		if len(allKeys) != numObjects {
			t.Errorf("Expected %d total objects, got %d", numObjects, len(allKeys))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, key := range allKeys {
			if seen[key] {
				t.Errorf("Duplicate key found: %s", key)
			}
			seen[key] = true
		}
	})

	// Test ListObjects (v1) pagination
	t.Run("ListObjects_Pagination", func(t *testing.T) {
		maxKeys := int32(4)
		var allKeys []string

		// First page
		output, err := ts.client.ListObjects(ts.ctx, &s3.ListObjectsInput{
			Bucket:  aws.String(bucketName),
			MaxKeys: aws.Int32(maxKeys),
		})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(output.Contents) > int(maxKeys) {
			t.Errorf("Expected at most %d objects, got %d", maxKeys, len(output.Contents))
		}

		for _, obj := range output.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		// Check IsTruncated flag
		if !*output.IsTruncated {
			t.Error("Expected IsTruncated=true for first page")
		}

		// Fetch remaining pages using marker
		for output.IsTruncated != nil && *output.IsTruncated && len(output.Contents) > 0 {
			marker := output.Contents[len(output.Contents)-1].Key
			output, err = ts.client.ListObjects(ts.ctx, &s3.ListObjectsInput{
				Bucket:  aws.String(bucketName),
				MaxKeys: aws.Int32(maxKeys),
				Marker:  marker,
			})
			if err != nil {
				t.Fatalf("ListObjects continuation failed: %v", err)
			}

			for _, obj := range output.Contents {
				allKeys = append(allKeys, *obj.Key)
			}
		}

		// Verify we got all objects
		if len(allKeys) != numObjects {
			t.Errorf("Expected %d total objects, got %d", numObjects, len(allKeys))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, key := range allKeys {
			if seen[key] {
				t.Errorf("Duplicate key found: %s", key)
			}
			seen[key] = true
		}
	})

	// Test StartAfter parameter
	t.Run("ListObjectsV2_StartAfter", func(t *testing.T) {
		startAfter := "object-004.txt"
		output, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket:     aws.String(bucketName),
			StartAfter: aws.String(startAfter),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 with StartAfter failed: %v", err)
		}

		// All returned objects should come after startAfter
		for _, obj := range output.Contents {
			if *obj.Key <= startAfter {
				t.Errorf("Object %s should come after StartAfter %s", *obj.Key, startAfter)
			}
		}

		// Should have 5 objects (005-009)
		expectedCount := 5
		if len(output.Contents) != expectedCount {
			t.Errorf("Expected %d objects after %s, got %d", expectedCount, startAfter, len(output.Contents))
		}
	})
}

// TestMultipartPagination tests pagination for multipart upload operations
func TestMultipartPagination(t *testing.T) {
	bucketName := "test-multipart-pagination"

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Initiate multiple uploads
	numUploads := 8
	uploadIDs := make([]*string, numUploads)
	for i := 0; i < numUploads; i++ {
		key := fmt.Sprintf("upload-%03d.txt", i)
		output, err := ts.client.CreateMultipartUpload(ts.ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("Failed to create multipart upload %d: %v", i, err)
		}
		uploadIDs[i] = output.UploadId
	}

	// Clean up at the end
	defer func() {
		for i, uploadID := range uploadIDs {
			if uploadID != nil {
				ts.client.AbortMultipartUpload(ts.ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucketName),
					Key:      aws.String(fmt.Sprintf("upload-%03d.txt", i)),
					UploadId: uploadID,
				})
			}
		}
	}()

	// Test ListMultipartUploads pagination
	t.Run("ListMultipartUploads_Pagination", func(t *testing.T) {
		maxUploads := int32(3)
		var allUploads []string

		// First page
		output, err := ts.client.ListMultipartUploads(ts.ctx, &s3.ListMultipartUploadsInput{
			Bucket:     aws.String(bucketName),
			MaxUploads: aws.Int32(maxUploads),
		})
		if err != nil {
			t.Fatalf("ListMultipartUploads failed: %v", err)
		}

		if len(output.Uploads) > int(maxUploads) {
			t.Errorf("Expected at most %d uploads, got %d", maxUploads, len(output.Uploads))
		}

		for _, upload := range output.Uploads {
			allUploads = append(allUploads, *upload.Key)
		}

		// Check IsTruncated flag
		if output.IsTruncated == nil || !*output.IsTruncated {
			t.Error("Expected IsTruncated=true for first page")
		}

		// Fetch remaining pages
		for output.IsTruncated != nil && *output.IsTruncated {
			output, err = ts.client.ListMultipartUploads(ts.ctx, &s3.ListMultipartUploadsInput{
				Bucket:         aws.String(bucketName),
				MaxUploads:     aws.Int32(maxUploads),
				KeyMarker:      output.NextKeyMarker,
				UploadIdMarker: output.NextUploadIdMarker,
			})
			if err != nil {
				t.Fatalf("ListMultipartUploads continuation failed: %v", err)
			}

			for _, upload := range output.Uploads {
				allUploads = append(allUploads, *upload.Key)
			}
		}

		// Verify we got all uploads
		if len(allUploads) != numUploads {
			t.Errorf("Expected %d total uploads, got %d", numUploads, len(allUploads))
		}

		// Verify no duplicates
		seen := make(map[string]bool)
		for _, key := range allUploads {
			if seen[key] {
				t.Errorf("Duplicate upload key found: %s", key)
			}
			seen[key] = true
		}
	})

	// Test ListParts pagination
	t.Run("ListParts_Pagination", func(t *testing.T) {
		objectKey := "multipart-object.txt"

		// Create a multipart upload
		initOutput, err := ts.client.CreateMultipartUpload(ts.ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("Failed to create multipart upload: %v", err)
		}
		uploadID := initOutput.UploadId

		defer ts.client.AbortMultipartUpload(ts.ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
		})

		// Upload multiple parts
		numParts := 8
		for i := 1; i <= numParts; i++ {
			_, err := ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(objectKey),
				UploadId:   uploadID,
				PartNumber: aws.Int32(int32(i)),
				Body:       strings.NewReader(fmt.Sprintf("part %d data", i)),
			})
			if err != nil {
				t.Fatalf("Failed to upload part %d: %v", i, err)
			}
		}

		// Test pagination
		maxParts := int32(3)
		var allParts []int32

		// First page
		output, err := ts.client.ListParts(ts.ctx, &s3.ListPartsInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MaxParts: aws.Int32(maxParts),
		})
		if err != nil {
			t.Fatalf("ListParts failed: %v", err)
		}

		if len(output.Parts) > int(maxParts) {
			t.Errorf("Expected at most %d parts, got %d", maxParts, len(output.Parts))
		}

		for _, part := range output.Parts {
			allParts = append(allParts, *part.PartNumber)
		}

		// Check IsTruncated flag
		if output.IsTruncated == nil || !*output.IsTruncated {
			t.Error("Expected IsTruncated=true for first page")
		}

		// Fetch remaining pages
		for output.IsTruncated != nil && *output.IsTruncated {
			output, err = ts.client.ListParts(ts.ctx, &s3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              aws.String(objectKey),
				UploadId:         uploadID,
				MaxParts:         aws.Int32(maxParts),
				PartNumberMarker: output.NextPartNumberMarker,
			})
			if err != nil {
				t.Fatalf("ListParts continuation failed: %v", err)
			}

			for _, part := range output.Parts {
				allParts = append(allParts, *part.PartNumber)
			}
		}

		// Verify we got all parts
		if len(allParts) != numParts {
			t.Errorf("Expected %d total parts, got %d", numParts, len(allParts))
		}

		// Verify parts are in order and no duplicates
		for i, partNum := range allParts {
			expectedPartNum := int32(i + 1)
			if partNum != expectedPartNum {
				t.Errorf("Expected part number %d at position %d, got %d", expectedPartNum, i, partNum)
			}
		}
	})
}
