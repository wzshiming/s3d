package integration

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestChecksumSHA256 tests SHA256 checksum functionality
func TestChecksumSHA256(t *testing.T) {
	bucketName := "test-checksum-sha256"
	objectKey := "checksum-test.txt"
	objectContent := "Hello, SHA256 Checksum!"

	// Calculate expected checksum
	hash := sha256.Sum256([]byte(objectContent))
	expectedChecksum := base64.StdEncoding.EncodeToString(hash[:])

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Test: PutObject with ChecksumAlgorithm
	t.Run("PutObjectWithChecksumAlgorithm", func(t *testing.T) {
		output, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket:            aws.String(bucketName),
			Key:               aws.String(objectKey),
			Body:              strings.NewReader(objectContent),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		})
		if err != nil {
			t.Fatalf("PutObject with ChecksumAlgorithm failed: %v", err)
		}

		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set")
		}
		if *output.ChecksumSHA256 != expectedChecksum {
			t.Errorf("ChecksumSHA256 mismatch: got %s, want %s", *output.ChecksumSHA256, expectedChecksum)
		}
		t.Logf("PutObject ChecksumSHA256: %s", *output.ChecksumSHA256)
	})

	// Test: PutObject with pre-calculated checksum
	t.Run("PutObjectWithPrecalculatedChecksum", func(t *testing.T) {
		newContent := "Different content for pre-calculated checksum"
		newHash := sha256.Sum256([]byte(newContent))
		newChecksum := base64.StdEncoding.EncodeToString(newHash[:])

		output, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String("precalc-checksum.txt"),
			Body:           strings.NewReader(newContent),
			ChecksumSHA256: aws.String(newChecksum),
		})
		if err != nil {
			t.Fatalf("PutObject with pre-calculated checksum failed: %v", err)
		}

		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set")
		}
		if *output.ChecksumSHA256 != newChecksum {
			t.Errorf("ChecksumSHA256 mismatch: got %s, want %s", *output.ChecksumSHA256, newChecksum)
		}
		t.Logf("PutObject with pre-calculated ChecksumSHA256: %s", *output.ChecksumSHA256)

		// Cleanup
		ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("precalc-checksum.txt"),
		})
	})

	// Test: GetObject with ChecksumMode enabled
	t.Run("GetObjectWithChecksumMode", func(t *testing.T) {
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(objectKey),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("GetObject with ChecksumMode failed: %v", err)
		}
		defer output.Body.Close()

		// Read body
		body, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read body: %v", err)
		}
		if string(body) != objectContent {
			t.Errorf("Content mismatch: got %s, want %s", string(body), objectContent)
		}

		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set in GetObject response")
		}
		if *output.ChecksumSHA256 != expectedChecksum {
			t.Errorf("ChecksumSHA256 mismatch: got %s, want %s", *output.ChecksumSHA256, expectedChecksum)
		}
		t.Logf("GetObject ChecksumSHA256: %s", *output.ChecksumSHA256)
	})

	// Test: HeadObject with ChecksumMode enabled
	t.Run("HeadObjectWithChecksumMode", func(t *testing.T) {
		output, err := ts.client.HeadObject(ts.ctx, &s3.HeadObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(objectKey),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("HeadObject with ChecksumMode failed: %v", err)
		}

		if output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 to be set in HeadObject response")
		}
		if *output.ChecksumSHA256 != expectedChecksum {
			t.Errorf("ChecksumSHA256 mismatch: got %s, want %s", *output.ChecksumSHA256, expectedChecksum)
		}
		t.Logf("HeadObject ChecksumSHA256: %s", *output.ChecksumSHA256)
	})

	// Test: PutObject with mismatched checksum should fail
	t.Run("PutObjectWithMismatchedChecksum", func(t *testing.T) {
		wrongChecksum := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String("mismatch-test.txt"),
			Body:           strings.NewReader("Some content"),
			ChecksumSHA256: aws.String(wrongChecksum),
		})
		if err == nil {
			t.Fatal("Expected PutObject with mismatched checksum to fail")
			// Cleanup if it unexpectedly succeeded
			ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("mismatch-test.txt"),
			})
		} else {
			t.Logf("PutObject with mismatched checksum correctly rejected: %v", err)
		}

		// Verify object was not created
		_, err = ts.client.HeadObject(ts.ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("mismatch-test.txt"),
		})
		if err == nil {
			t.Fatal("Object should not exist after checksum mismatch")
		}
	})

	// Cleanup
	ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
}

// TestMultipartUploadChecksum tests checksum functionality with multipart uploads
func TestMultipartUploadChecksum(t *testing.T) {
	bucketName := "test-multipart-checksum"
	objectKey := "multipart-checksum.txt"
	part1Content := "Part 1 content for multipart upload"
	part2Content := "Part 2 content for multipart upload"

	// Calculate checksums for each part
	part1Hash := sha256.Sum256([]byte(part1Content))
	part1Checksum := base64.StdEncoding.EncodeToString(part1Hash[:])
	part2Hash := sha256.Sum256([]byte(part2Content))
	part2Checksum := base64.StdEncoding.EncodeToString(part2Hash[:])

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Test: Multipart upload with checksums
	t.Run("MultipartUploadWithChecksum", func(t *testing.T) {
		// Initiate multipart upload
		createOutput, err := ts.client.CreateMultipartUpload(ts.ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}
		uploadID := createOutput.UploadId

		// Upload part 1 with checksum
		part1Output, err := ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String(objectKey),
			UploadId:       uploadID,
			PartNumber:     aws.Int32(1),
			Body:           strings.NewReader(part1Content),
			ChecksumSHA256: aws.String(part1Checksum),
		})
		if err != nil {
			t.Fatalf("UploadPart 1 failed: %v", err)
		}
		if part1Output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 for part 1")
		}
		if *part1Output.ChecksumSHA256 != part1Checksum {
			t.Errorf("Part 1 checksum mismatch: got %s, want %s", *part1Output.ChecksumSHA256, part1Checksum)
		}
		t.Logf("Part 1 ChecksumSHA256: %s", *part1Output.ChecksumSHA256)

		// Upload part 2 with checksum
		part2Output, err := ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String(objectKey),
			UploadId:       uploadID,
			PartNumber:     aws.Int32(2),
			Body:           strings.NewReader(part2Content),
			ChecksumSHA256: aws.String(part2Checksum),
		})
		if err != nil {
			t.Fatalf("UploadPart 2 failed: %v", err)
		}
		if part2Output.ChecksumSHA256 == nil {
			t.Fatal("Expected ChecksumSHA256 for part 2")
		}
		t.Logf("Part 2 ChecksumSHA256: %s", *part2Output.ChecksumSHA256)

		// Complete multipart upload
		completeOutput, err := ts.client.CompleteMultipartUpload(ts.ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: part1Output.ETag},
					{PartNumber: aws.Int32(2), ETag: part2Output.ETag},
				},
			},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload failed: %v", err)
		}
		t.Logf("CompleteMultipartUpload ETag: %s", *completeOutput.ETag)

		// Verify object was created
		headOutput, err := ts.client.HeadObject(ts.ctx, &s3.HeadObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(objectKey),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("HeadObject after multipart upload failed: %v", err)
		}
		if headOutput.ChecksumSHA256 != nil {
			t.Logf("Final object ChecksumSHA256: %s", *headOutput.ChecksumSHA256)
		}

		// Cleanup
		ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	})

	// Test: Upload part with mismatched checksum should fail
	t.Run("UploadPartWithMismatchedChecksum", func(t *testing.T) {
		// Initiate multipart upload
		createOutput, err := ts.client.CreateMultipartUpload(ts.ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("mismatch-multipart.txt"),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}
		uploadID := createOutput.UploadId

		// Try to upload part with wrong checksum
		wrongChecksum := "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="
		_, err = ts.client.UploadPart(ts.ctx, &s3.UploadPartInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String("mismatch-multipart.txt"),
			UploadId:       uploadID,
			PartNumber:     aws.Int32(1),
			Body:           strings.NewReader("Some part content"),
			ChecksumSHA256: aws.String(wrongChecksum),
		})
		if err == nil {
			t.Fatal("Expected UploadPart with mismatched checksum to fail")
		} else {
			t.Logf("UploadPart with mismatched checksum correctly rejected: %v", err)
		}

		// Cleanup - abort the multipart upload
		ts.client.AbortMultipartUpload(ts.ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String("mismatch-multipart.txt"),
			UploadId: uploadID,
		})
	})
}
