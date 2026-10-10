package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestObjectTagging(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-object-tagging"
	objectKey := "test-tagging.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create object without tags
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("tagging test content"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	t.Run("GetObjectTaggingEmpty", func(t *testing.T) {
		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 0 {
			t.Fatalf("Expected empty tag set, got %d tags", len(output.TagSet))
		}
	})

	t.Run("PutAndGetObjectTagging", func(t *testing.T) {
		_, err := ts.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{
					{Key: aws.String("0"), Value: aws.String("0")},
					{Key: aws.String("1"), Value: aws.String("1")},
				},
			},
		})
		if err != nil {
			t.Fatalf("PutObjectTagging failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 2 {
			t.Fatalf("Expected 2 tags, got %d", len(output.TagSet))
		}
		for i, expected := range []string{"0", "1"} {
			if *output.TagSet[i].Key != expected || *output.TagSet[i].Value != expected {
				t.Fatalf("Expected tag %d to be %s=%s, got %s=%s", i, expected, expected, *output.TagSet[i].Key, *output.TagSet[i].Value)
			}
		}
	})

	t.Run("HeadObjectTaggingCount", func(t *testing.T) {
		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}
		if output.TagCount == nil || *output.TagCount != 2 {
			t.Fatalf("Expected x-amz-tagging-count of 2, got %v", output.TagCount)
		}
	})

	t.Run("GetObjectTaggingCount", func(t *testing.T) {
		output, err := ts.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer output.Body.Close()
		if output.TagCount == nil || *output.TagCount != 2 {
			t.Fatalf("Expected x-amz-tagging-count of 2, got %v", output.TagCount)
		}
	})

	t.Run("DeleteObjectTagging", func(t *testing.T) {
		_, err := ts.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("DeleteObjectTagging failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 0 {
			t.Fatalf("Expected empty tag set after delete, got %d tags", len(output.TagSet))
		}

		// HeadObject should no longer report a tag count
		headOutput, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}
		if headOutput.TagCount != nil {
			t.Fatalf("Expected no x-amz-tagging-count after delete, got %v", *headOutput.TagCount)
		}
	})

	t.Run("PutObjectWithTaggingHeader", func(t *testing.T) {
		headerKey := "test-tagging-header.txt"
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(bucketName),
			Key:     aws.String(headerKey),
			Body:    strings.NewReader("tagging header content"),
			Tagging: aws.String("foo=bar&bar"),
		})
		if err != nil {
			t.Fatalf("PutObject with tagging failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(headerKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 2 {
			t.Fatalf("Expected 2 tags, got %d", len(output.TagSet))
		}
		// Tags are returned sorted by key
		if *output.TagSet[0].Key != "bar" || *output.TagSet[0].Value != "" {
			t.Fatalf("Expected first tag bar=<empty>, got %s=%s", *output.TagSet[0].Key, *output.TagSet[0].Value)
		}
		if *output.TagSet[1].Key != "foo" || *output.TagSet[1].Value != "bar" {
			t.Fatalf("Expected second tag foo=bar, got %s=%s", *output.TagSet[1].Key, *output.TagSet[1].Value)
		}
	})

	t.Run("PutObjectWithExcessTagsHeader", func(t *testing.T) {
		tagPairs := make([]string, 11)
		for i := range tagPairs {
			tagPairs[i] = fmt.Sprintf("key%d=value%d", i, i)
		}
		_, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(bucketName),
			Key:     aws.String("test-excess-header.txt"),
			Body:    strings.NewReader("content"),
			Tagging: aws.String(strings.Join(tagPairs, "&")),
		})
		if err == nil {
			t.Fatal("Expected error for more than 10 tags in x-amz-tagging header, got nil")
		}
		if !strings.Contains(err.Error(), "InvalidTag") {
			t.Fatalf("Expected InvalidTag error, got: %v", err)
		}
	})

	t.Run("PutObjectTaggingExcessTags", func(t *testing.T) {
		tagSet := make([]types.Tag, 11)
		for i := range tagSet {
			tagSet[i] = types.Tag{Key: aws.String(fmt.Sprintf("%d", i)), Value: aws.String(fmt.Sprintf("%d", i))}
		}
		_, err := ts.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket:  aws.String(bucketName),
			Key:     aws.String(objectKey),
			Tagging: &types.Tagging{TagSet: tagSet},
		})
		if err == nil {
			t.Fatal("Expected error for more than 10 tags, got nil")
		}
		if !strings.Contains(err.Error(), "InvalidTag") {
			t.Fatalf("Expected InvalidTag error, got: %v", err)
		}

		// Tags must remain unchanged
		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 0 {
			t.Fatalf("Expected empty tag set after failed put, got %d tags", len(output.TagSet))
		}
	})

	t.Run("PutObjectTaggingExcessKeyLength", func(t *testing.T) {
		_, err := ts.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Tagging: &types.Tagging{TagSet: []types.Tag{
				{Key: aws.String(strings.Repeat("k", 129)), Value: aws.String("value")},
			}},
		})
		if err == nil {
			t.Fatal("Expected error for tag key longer than 128 characters, got nil")
		}
		if !strings.Contains(err.Error(), "InvalidTag") {
			t.Fatalf("Expected InvalidTag error, got: %v", err)
		}
	})

	t.Run("PutObjectTaggingExcessValueLength", func(t *testing.T) {
		_, err := ts.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Tagging: &types.Tagging{TagSet: []types.Tag{
				{Key: aws.String("key"), Value: aws.String(strings.Repeat("v", 257))},
			}},
		})
		if err == nil {
			t.Fatal("Expected error for tag value longer than 256 characters, got nil")
		}
		if !strings.Contains(err.Error(), "InvalidTag") {
			t.Fatalf("Expected InvalidTag error, got: %v", err)
		}
	})

	t.Run("PutObjectTaggingDuplicateKeys", func(t *testing.T) {
		_, err := ts.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Tagging: &types.Tagging{TagSet: []types.Tag{
				{Key: aws.String("key"), Value: aws.String("value1")},
				{Key: aws.String("key"), Value: aws.String("value2")},
			}},
		})
		if err == nil {
			t.Fatal("Expected error for duplicate tag keys, got nil")
		}
		if !strings.Contains(err.Error(), "InvalidTag") {
			t.Fatalf("Expected InvalidTag error, got: %v", err)
		}
	})

	t.Run("PutObjectTaggingMaxSizes", func(t *testing.T) {
		tagSet := make([]types.Tag, 10)
		for i := range tagSet {
			tagSet[i] = types.Tag{
				Key:   aws.String(fmt.Sprintf("%d%s", i, strings.Repeat("k", 127))),
				Value: aws.String(strings.Repeat("v", 256)),
			}
		}
		_, err := ts.client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket:  aws.String(bucketName),
			Key:     aws.String(objectKey),
			Tagging: &types.Tagging{TagSet: tagSet},
		})
		if err != nil {
			t.Fatalf("PutObjectTagging with max sizes failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 10 {
			t.Fatalf("Expected 10 tags, got %d", len(output.TagSet))
		}
	})

	t.Run("GetObjectTaggingNoSuchKey", func(t *testing.T) {
		_, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("nonexistent-object"),
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent object, got nil")
		}
		if !strings.Contains(err.Error(), "NoSuchKey") && !strings.Contains(err.Error(), "404") {
			t.Fatalf("Expected NoSuchKey error, got: %v", err)
		}
	})
}

func TestMultipartUploadTagging(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-multipart-tagging"
	objectKey := "test-multipart-tagging.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Initiate multipart upload with tagging
	initOutput, err := ts.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Tagging: aws.String("Hello=World&foo=bar"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	partOutput, err := ts.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		UploadId:   initOutput.UploadId,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("multipart tagging content"),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	_, err = ts.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: initOutput.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: partOutput.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify tags on the completed object
	output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging failed: %v", err)
	}
	if len(output.TagSet) != 2 {
		t.Fatalf("Expected 2 tags, got %d", len(output.TagSet))
	}
	if *output.TagSet[0].Key != "Hello" || *output.TagSet[0].Value != "World" {
		t.Fatalf("Expected first tag Hello=World, got %s=%s", *output.TagSet[0].Key, *output.TagSet[0].Value)
	}
	if *output.TagSet[1].Key != "foo" || *output.TagSet[1].Value != "bar" {
		t.Fatalf("Expected second tag foo=bar, got %s=%s", *output.TagSet[1].Key, *output.TagSet[1].Value)
	}

	// Delete the tags
	_, err = ts.client.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("DeleteObjectTagging failed: %v", err)
	}

	output, err = ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("GetObjectTagging failed: %v", err)
	}
	if len(output.TagSet) != 0 {
		t.Fatalf("Expected empty tag set after delete, got %d tags", len(output.TagSet))
	}
}

func TestCopyObjectTagging(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-copy-tagging"
	srcKey := "src-object.txt"

	// Create bucket
	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// Create source object with tags
	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(srcKey),
		Body:    strings.NewReader("copy tagging content"),
		Tagging: aws.String("srckey=srcvalue"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	t.Run("CopyDefaultCopiesTags", func(t *testing.T) {
		dstKey := "dst-copy-tags.txt"
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(dstKey),
			CopySource: aws.String(bucketName + "/" + srcKey),
		})
		if err != nil {
			t.Fatalf("CopyObject failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dstKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 1 || *output.TagSet[0].Key != "srckey" || *output.TagSet[0].Value != "srcvalue" {
			t.Fatalf("Expected copied tag srckey=srcvalue, got %v", output.TagSet)
		}
	})

	t.Run("CopyReplaceTaggingDirective", func(t *testing.T) {
		dstKey := "dst-replace-tags.txt"
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:           aws.String(bucketName),
			Key:              aws.String(dstKey),
			CopySource:       aws.String(bucketName + "/" + srcKey),
			TaggingDirective: types.TaggingDirectiveReplace,
			Tagging:          aws.String("newkey=newvalue"),
		})
		if err != nil {
			t.Fatalf("CopyObject with TaggingDirective REPLACE failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dstKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 1 || *output.TagSet[0].Key != "newkey" || *output.TagSet[0].Value != "newvalue" {
			t.Fatalf("Expected replaced tag newkey=newvalue, got %v", output.TagSet)
		}
	})

	t.Run("CopyReplaceMetadataKeepsTags", func(t *testing.T) {
		dstKey := "dst-replace-metadata.txt"
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(bucketName),
			Key:               aws.String(dstKey),
			CopySource:        aws.String(bucketName + "/" + srcKey),
			MetadataDirective: types.MetadataDirectiveReplace,
			Metadata:          map[string]string{"newmeta": "newvalue"},
		})
		if err != nil {
			t.Fatalf("CopyObject with MetadataDirective REPLACE failed: %v", err)
		}

		output, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dstKey),
		})
		if err != nil {
			t.Fatalf("GetObjectTagging failed: %v", err)
		}
		if len(output.TagSet) != 1 || *output.TagSet[0].Key != "srckey" || *output.TagSet[0].Value != "srcvalue" {
			t.Fatalf("Expected copied tag srckey=srcvalue, got %v", output.TagSet)
		}
	})
}
