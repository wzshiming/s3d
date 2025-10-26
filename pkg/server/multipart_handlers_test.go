package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestMultipartUpload(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multipart-upload"
	objectKey := "test-multipart.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Test multipart upload
	t.Run("MultipartUpload", func(t *testing.T) {
		// Initiate multipart upload
		initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		uploadID := initOutput.UploadId

		// Upload parts
		part1Data := "Part 1 content - this is the first part of the multipart upload"
		part2Data := "Part 2 content - this is the second part of the multipart upload"

		part1Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       strings.NewReader(part1Data),
		})
		if err != nil {
			t.Fatalf("UploadPart 1 failed: %v", err)
		}

		part2Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       strings.NewReader(part2Data),
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
						PartNumber: aws.Int32(1),
						ETag:       part1Output.ETag,
					},
					{
						PartNumber: aws.Int32(2),
						ETag:       part2Output.ETag,
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}

		// Verify the object
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read object body: %v", err)
		}

		expectedContent := part1Data + part2Data
		if string(data) != expectedContent {
			t.Fatalf("Expected content %q, got %q", expectedContent, string(data))
		}
	})
}

func TestListMultipartUploads(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-multipart-uploads"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multiple uploads
	uploads := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, key := range uploads {
		_, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}
	}

	// List multipart uploads
	output, err := ts.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads failed: %v", err)
	}

	if len(output.Uploads) != 3 {
		t.Fatalf("Expected 3 uploads, got %d", len(output.Uploads))
	}
}

func TestListParts(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-parts"
	objectKey := "test-parts.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := initOutput.UploadId

	// Upload parts
	partData := []string{"part1 data", "part2 data", "part3 data"}
	for i, data := range partData {
		_, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadID,
			PartNumber: aws.Int32(int32(i + 1)),
			Body:       strings.NewReader(data),
		})
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", i+1, err)
		}
	}

	// List parts
	output, err := ts.client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
	})
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}

	if len(output.Parts) != 3 {
		t.Fatalf("Expected 3 parts, got %d", len(output.Parts))
	}

	// Verify parts are in order
	for i, part := range output.Parts {
		if *part.PartNumber != int32(i+1) {
			t.Fatalf("Expected part number %d, got %d", i+1, *part.PartNumber)
		}
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-abort-multipart-upload"
	objectKey := "test-abort.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := initOutput.UploadId

	// Upload a part
	_, err = ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("test data"),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort multipart upload
	_, err = ts.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify upload is aborted - trying to upload another part should fail
	_, err = ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader("more data"),
	})
	if err == nil {
		t.Fatal("Expected error when uploading to aborted upload")
	}
}

func TestUploadPartInvalidUploadID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-part-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &bucketName,
		Key:        aws.String("test.txt"),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String("invalid-upload-id"),
		Body:       bytes.NewReader([]byte("data")),
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestCompleteMultipartUploadInvalidID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-complete-multipart-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &bucketName,
		Key:      aws.String("test.txt"),
		UploadId: aws.String("invalid-upload-id"),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{},
		},
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestAbortMultipartUploadInvalidID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-abort-multipart-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &bucketName,
		Key:      aws.String("test.txt"),
		UploadId: aws.String("invalid-upload-id"),
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestListPartsInvalidUploadID(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-parts-invalid-id"
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   &bucketName,
		Key:      aws.String("test.txt"),
		UploadId: aws.String("invalid-upload-id"),
	})

	if err == nil {
		t.Fatal("Expected error for invalid upload ID")
	}
}

func TestUploadPartCopy(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-part-copy"
	sourceKey := "source-object.txt"
	destKey := "dest-multipart.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object
	sourceContent := "This is the source object content for UploadPartCopy test"
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(sourceKey),
		Body:   strings.NewReader(sourceContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(destKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := initOutput.UploadId

	// Upload part 1 using regular UploadPart
	part1Data := "Part 1 - regular upload"
	part1Output, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader(part1Data),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	// Upload part 2 using UploadPartCopy
	copySource := fmt.Sprintf("%s/%s", bucketName, sourceKey)
	part2Output, err := ts.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(2),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		t.Fatalf("UploadPartCopy failed: %v", err)
	}

	if part2Output.CopyPartResult == nil || part2Output.CopyPartResult.ETag == nil {
		t.Fatal("UploadPartCopy did not return ETag")
	}

	// Complete multipart upload
	_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(destKey),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					PartNumber: aws.Int32(1),
					ETag:       part1Output.ETag,
				},
				{
					PartNumber: aws.Int32(2),
					ETag:       part2Output.CopyPartResult.ETag,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify the final object
	output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(destKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("Failed to read object body: %v", err)
	}

	expectedContent := part1Data + sourceContent
	if string(data) != expectedContent {
		t.Fatalf("Expected content %q, got %q", expectedContent, string(data))
	}
}

func TestUploadPartCopyNonExistentSource(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-part-copy-no-src"
	destKey := "dest.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(destKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Try to copy from non-existent source
	copySource := fmt.Sprintf("%s/non-existent.txt", bucketName)
	_, err = ts.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		UploadId:   initOutput.UploadId,
		PartNumber: aws.Int32(1),
		CopySource: aws.String(copySource),
	})

	if err == nil {
		t.Fatal("Expected error when copying from non-existent source")
	}
}

func TestUploadPartCopyWithSpecialCharacters(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-upload-part-copy-special"
	sourceKey := "source object with spaces.txt"
	destKey := "dest-multipart.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object with special characters in name
	sourceContent := "Content for object with special characters"
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(sourceKey),
		Body:   strings.NewReader(sourceContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(destKey),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := initOutput.UploadId

	// Upload part using UploadPartCopy with URL-encoded source
	copySource := fmt.Sprintf("%s/%s", bucketName, sourceKey)
	part1Output, err := ts.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		UploadId:   uploadID,
		PartNumber: aws.Int32(1),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		t.Fatalf("UploadPartCopy failed: %v", err)
	}

	if part1Output.CopyPartResult == nil || part1Output.CopyPartResult.ETag == nil {
		t.Fatal("UploadPartCopy did not return ETag")
	}

	// Complete multipart upload
	_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(destKey),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					PartNumber: aws.Int32(1),
					ETag:       part1Output.CopyPartResult.ETag,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify the final object
	output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(destKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatalf("Failed to read object body: %v", err)
	}

	if string(data) != sourceContent {
		t.Fatalf("Expected content %q, got %q", sourceContent, string(data))
	}
}

func TestListMultipartUploadsPagination(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-multipart-pagination"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Create multiple uploads - more than we'll request per page
	numUploads := 8
	uploadIDs := make([]*string, numUploads)
	for i := 0; i < numUploads; i++ {
		key := fmt.Sprintf("upload-%03d.txt", i)
		initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("Failed to create multipart upload %d: %v", i, err)
		}
		uploadIDs[i] = initOutput.UploadId
	}

	// Clean up uploads at the end
	defer func() {
		for i, uploadID := range uploadIDs {
			if uploadID != nil {
				ts.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucketName),
					Key:      aws.String(fmt.Sprintf("upload-%03d.txt", i)),
					UploadId: uploadID,
				})
			}
		}
	}()

	// Test pagination with MaxUploads
	t.Run("PaginationWithMaxUploads", func(t *testing.T) {
		maxUploads := int32(3)
		var allUploads []string

		// First page
		output, err := ts.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:     aws.String(bucketName),
			MaxUploads: aws.Int32(maxUploads),
		})
		if err != nil {
			t.Fatalf("ListMultipartUploads first page failed: %v", err)
		}

		if len(output.Uploads) > int(maxUploads) {
			t.Errorf("Expected at most %d uploads in first page, got %d", maxUploads, len(output.Uploads))
		}

		for _, upload := range output.Uploads {
			allUploads = append(allUploads, *upload.Key)
		}

		// If there are more uploads, IsTruncated should be true
		if numUploads > int(maxUploads) && (output.IsTruncated == nil || !*output.IsTruncated) {
			t.Errorf("Expected IsTruncated=true when more uploads exist than MaxUploads")
		}

		// Continue fetching pages if truncated
		keyMarker := output.NextKeyMarker
		uploadIDMarker := output.NextUploadIdMarker
		for output.IsTruncated != nil && *output.IsTruncated {
			output, err = ts.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
				Bucket:         aws.String(bucketName),
				MaxUploads:     aws.Int32(maxUploads),
				KeyMarker:      keyMarker,
				UploadIdMarker: uploadIDMarker,
			})
			if err != nil {
				t.Fatalf("ListMultipartUploads continuation failed: %v", err)
			}

			for _, upload := range output.Uploads {
				allUploads = append(allUploads, *upload.Key)
			}

			keyMarker = output.NextKeyMarker
			uploadIDMarker = output.NextUploadIdMarker
		}

		// Verify we got all uploads
		if len(allUploads) != numUploads {
			t.Errorf("Expected %d total uploads across all pages, got %d", numUploads, len(allUploads))
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

	// Test small MaxUploads value
	t.Run("MaxUploadsOne", func(t *testing.T) {
		output, err := ts.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:     aws.String(bucketName),
			MaxUploads: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("ListMultipartUploads with MaxUploads=1 failed: %v", err)
		}

		if len(output.Uploads) != 1 {
			t.Errorf("Expected exactly 1 upload with MaxUploads=1, got %d", len(output.Uploads))
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			t.Errorf("Expected IsTruncated=true with MaxUploads=1 and %d total uploads", numUploads)
		}

		if output.NextKeyMarker == nil {
			t.Errorf("Expected NextKeyMarker to be set when IsTruncated=true")
		}
	})
}

func TestListPartsPagination(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-list-parts-pagination"
	objectKey := "test-multipart-object.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Initiate multipart upload
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("Failed to create multipart upload: %v", err)
	}
	uploadID := initOutput.UploadId

	defer ts.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
	})

	// Upload multiple parts - more than we'll request per page
	numParts := 8
	for i := 1; i <= numParts; i++ {
		_, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
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

	// Test pagination with MaxParts
	t.Run("PaginationWithMaxParts", func(t *testing.T) {
		maxParts := int32(3)
		var allParts []int32

		// First page
		output, err := ts.client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MaxParts: aws.Int32(maxParts),
		})
		if err != nil {
			t.Fatalf("ListParts first page failed: %v", err)
		}

		if len(output.Parts) > int(maxParts) {
			t.Errorf("Expected at most %d parts in first page, got %d", maxParts, len(output.Parts))
		}

		for _, part := range output.Parts {
			allParts = append(allParts, *part.PartNumber)
		}

		// If there are more parts, IsTruncated should be true
		if numParts > int(maxParts) && (output.IsTruncated == nil || !*output.IsTruncated) {
			t.Errorf("Expected IsTruncated=true when more parts exist than MaxParts")
		}

		// Continue fetching pages if truncated
		partNumberMarker := output.NextPartNumberMarker
		for output.IsTruncated != nil && *output.IsTruncated {
			output, err = ts.client.ListParts(ctx, &s3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              aws.String(objectKey),
				UploadId:         uploadID,
				MaxParts:         aws.Int32(maxParts),
				PartNumberMarker: partNumberMarker,
			})
			if err != nil {
				t.Fatalf("ListParts continuation failed: %v", err)
			}

			for _, part := range output.Parts {
				allParts = append(allParts, *part.PartNumber)
			}

			partNumberMarker = output.NextPartNumberMarker
		}

		// Verify we got all parts
		if len(allParts) != numParts {
			t.Errorf("Expected %d total parts across all pages, got %d", numParts, len(allParts))
		}

		// Verify no duplicates and parts are in order
		for i, partNum := range allParts {
			expectedPartNum := int32(i + 1)
			if partNum != expectedPartNum {
				t.Errorf("Expected part number %d at position %d, got %d", expectedPartNum, i, partNum)
			}
		}
	})

	// Test small MaxParts value
	t.Run("MaxPartsOne", func(t *testing.T) {
		output, err := ts.client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MaxParts: aws.Int32(1),
		})
		if err != nil {
			t.Fatalf("ListParts with MaxParts=1 failed: %v", err)
		}

		if len(output.Parts) != 1 {
			t.Errorf("Expected exactly 1 part with MaxParts=1, got %d", len(output.Parts))
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			t.Errorf("Expected IsTruncated=true with MaxParts=1 and %d total parts", numParts)
		}

		if output.NextPartNumberMarker == nil {
			t.Errorf("Expected NextPartNumberMarker to be set when IsTruncated=true")
		}
	})
}
