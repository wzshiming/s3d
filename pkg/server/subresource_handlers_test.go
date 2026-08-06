package server

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func apiErrorCode(t *testing.T, err error) string {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Expected API error, got: %v", err)
	}
	return apiErr.ErrorCode()
}

func TestBucketSubresources(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-bucket-subresources"

	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	t.Run("GetBucketVersioning_Unset", func(t *testing.T) {
		output, err := ts.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketVersioning failed: %v", err)
		}
		if output.Status != "" {
			t.Fatalf("Expected empty versioning status, got %q", output.Status)
		}
	})

	t.Run("PutGetBucketVersioning", func(t *testing.T) {
		_, err := ts.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucketName),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		if err != nil {
			t.Fatalf("PutBucketVersioning failed: %v", err)
		}

		output, err := ts.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketVersioning failed: %v", err)
		}
		if output.Status != s3types.BucketVersioningStatusEnabled {
			t.Fatalf("Expected versioning status Enabled, got %q", output.Status)
		}
	})

	t.Run("GetBucketTagging_Unset", func(t *testing.T) {
		_, err := ts.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "NoSuchTagSet" {
			t.Fatalf("Expected NoSuchTagSet, got %q", code)
		}
	})

	t.Run("PutGetDeleteBucketTagging", func(t *testing.T) {
		_, err := ts.client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
			Bucket: aws.String(bucketName),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("env"), Value: aws.String("test")},
				},
			},
		})
		if err != nil {
			t.Fatalf("PutBucketTagging failed: %v", err)
		}

		output, err := ts.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketTagging failed: %v", err)
		}
		if len(output.TagSet) != 1 || *output.TagSet[0].Key != "env" || *output.TagSet[0].Value != "test" {
			t.Fatalf("Unexpected tag set: %+v", output.TagSet)
		}

		_, err = ts.client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("DeleteBucketTagging failed: %v", err)
		}

		_, err = ts.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "NoSuchTagSet" {
			t.Fatalf("Expected NoSuchTagSet after delete, got %q", code)
		}
	})

	t.Run("GetBucketPolicy", func(t *testing.T) {
		_, err := ts.client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "NoSuchBucketPolicy" {
			t.Fatalf("Expected NoSuchBucketPolicy, got %q", code)
		}
	})

	t.Run("GetBucketLifecycle", func(t *testing.T) {
		_, err := ts.client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "NoSuchLifecycleConfiguration" {
			t.Fatalf("Expected NoSuchLifecycleConfiguration, got %q", code)
		}
	})

	t.Run("GetBucketEncryption", func(t *testing.T) {
		_, err := ts.client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "ServerSideEncryptionConfigurationNotFoundError" {
			t.Fatalf("Expected ServerSideEncryptionConfigurationNotFoundError, got %q", code)
		}
	})

	t.Run("GetPublicAccessBlock", func(t *testing.T) {
		_, err := ts.client.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "NoSuchPublicAccessBlockConfiguration" {
			t.Fatalf("Expected NoSuchPublicAccessBlockConfiguration, got %q", code)
		}
	})

	t.Run("GetBucketCors", func(t *testing.T) {
		_, err := ts.client.GetBucketCors(ctx, &s3.GetBucketCorsInput{
			Bucket: aws.String(bucketName),
		})
		if code := apiErrorCode(t, err); code != "NoSuchCORSConfiguration" {
			t.Fatalf("Expected NoSuchCORSConfiguration, got %q", code)
		}
	})

	t.Run("GetBucketAcl", func(t *testing.T) {
		output, err := ts.client.GetBucketAcl(ctx, &s3.GetBucketAclInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketAcl failed: %v", err)
		}
		if len(output.Grants) != 1 || output.Grants[0].Permission != s3types.PermissionFullControl {
			t.Fatalf("Unexpected grants: %+v", output.Grants)
		}
	})

	t.Run("PutBucketAcl", func(t *testing.T) {
		_, err := ts.client.PutBucketAcl(ctx, &s3.PutBucketAclInput{
			Bucket: aws.String(bucketName),
			ACL:    s3types.BucketCannedACLPrivate,
		})
		if err != nil {
			t.Fatalf("PutBucketAcl failed: %v", err)
		}
	})

	t.Run("GetBucketLocation", func(t *testing.T) {
		output, err := ts.client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("GetBucketLocation failed: %v", err)
		}
		// us-east-1 is represented by an empty LocationConstraint
		if output.LocationConstraint != "" {
			t.Fatalf("Expected empty location constraint, got %q", output.LocationConstraint)
		}
	})

	t.Run("PutBucketPolicy_NotImplemented", func(t *testing.T) {
		_, err := ts.client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucketName),
			Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		if code := apiErrorCode(t, err); code != "NotImplemented" {
			t.Fatalf("Expected NotImplemented, got %q", code)
		}
	})

	t.Run("Subresource_NoSuchBucket", func(t *testing.T) {
		_, err := ts.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String("no-such-bucket-subresource"),
		})
		if code := apiErrorCode(t, err); code != "NoSuchBucket" {
			t.Fatalf("Expected NoSuchBucket, got %q", code)
		}
	})

	t.Run("ObjectSubresource_NotImplemented", func(t *testing.T) {
		_, err := ts.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("some-key"),
		})
		if code := apiErrorCode(t, err); code != "NotImplemented" {
			t.Fatalf("Expected NotImplemented, got %q", code)
		}
	})

	// Cleanup
	_, err = ts.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
}
