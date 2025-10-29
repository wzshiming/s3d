package integration

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestBucketOwnershipControls tests bucket ownership control operations
func TestBucketOwnershipControls(t *testing.T) {
	bucketName := "test-bucket-ownership"

	// Create bucket
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer ts.client.DeleteBucket(ts.ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	// Test: Get default ownership controls
	t.Run("GetOwnership_Default", func(t *testing.T) {
		output, err := ts.client.GetBucketOwnershipControls(ts.ctx, &s3.GetBucketOwnershipControlsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket ownership controls: %v", err)
		}

		if output.OwnershipControls == nil {
			t.Fatal("Expected OwnershipControls to be set")
		}

		if len(output.OwnershipControls.Rules) == 0 {
			t.Fatal("Expected at least one rule")
		}

		if output.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerEnforced {
			t.Errorf("Expected default ownership to be BucketOwnerEnforced, got %v", output.OwnershipControls.Rules[0].ObjectOwnership)
		}
	})

	// Test: Put ownership controls
	t.Run("PutOwnership", func(t *testing.T) {
		_, err := ts.client.PutBucketOwnershipControls(ts.ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucketName),
			OwnershipControls: &types.OwnershipControls{
				Rules: []types.OwnershipControlsRule{
					{
						ObjectOwnership: types.ObjectOwnershipBucketOwnerPreferred,
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("Failed to put bucket ownership controls: %v", err)
		}
	})

	// Test: Get updated ownership controls
	t.Run("GetOwnership_Updated", func(t *testing.T) {
		output, err := ts.client.GetBucketOwnershipControls(ts.ctx, &s3.GetBucketOwnershipControlsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket ownership controls: %v", err)
		}

		if output.OwnershipControls == nil {
			t.Fatal("Expected OwnershipControls to be set")
		}

		if len(output.OwnershipControls.Rules) == 0 {
			t.Fatal("Expected at least one rule")
		}

		if output.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerPreferred {
			t.Errorf("Expected ownership to be BucketOwnerPreferred, got %v", output.OwnershipControls.Rules[0].ObjectOwnership)
		}
	})

	// Test: Delete ownership controls
	t.Run("DeleteOwnership", func(t *testing.T) {
		_, err := ts.client.DeleteBucketOwnershipControls(ts.ctx, &s3.DeleteBucketOwnershipControlsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to delete bucket ownership controls: %v", err)
		}
	})

	// Test: Get ownership after deletion (should revert to default)
	t.Run("GetOwnership_AfterDeletion", func(t *testing.T) {
		output, err := ts.client.GetBucketOwnershipControls(ts.ctx, &s3.GetBucketOwnershipControlsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to get bucket ownership controls: %v", err)
		}

		if output.OwnershipControls == nil {
			t.Fatal("Expected OwnershipControls to be set")
		}

		if len(output.OwnershipControls.Rules) == 0 {
			t.Fatal("Expected at least one rule")
		}

		if output.OwnershipControls.Rules[0].ObjectOwnership != types.ObjectOwnershipBucketOwnerEnforced {
			t.Errorf("Expected ownership to revert to BucketOwnerEnforced, got %v", output.OwnershipControls.Rules[0].ObjectOwnership)
		}
	})

	// Test: Ownership controls for non-existent bucket
	t.Run("GetOwnership_NotFound", func(t *testing.T) {
		_, err := ts.client.GetBucketOwnershipControls(ts.ctx, &s3.GetBucketOwnershipControlsInput{
			Bucket: aws.String("non-existent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error for non-existent bucket, got nil")
		}
	})

	// Test: Put ownership controls for non-existent bucket
	t.Run("PutOwnership_NotFound", func(t *testing.T) {
		_, err := ts.client.PutBucketOwnershipControls(ts.ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String("non-existent-bucket"),
			OwnershipControls: &types.OwnershipControls{
				Rules: []types.OwnershipControlsRule{
					{
						ObjectOwnership: types.ObjectOwnershipBucketOwnerPreferred,
					},
				},
			},
		})
		if err == nil {
			t.Fatal("Expected error for non-existent bucket, got nil")
		}
	})

	// Test: Delete ownership controls for non-existent bucket
	t.Run("DeleteOwnership_NotFound", func(t *testing.T) {
		_, err := ts.client.DeleteBucketOwnershipControls(ts.ctx, &s3.DeleteBucketOwnershipControlsInput{
			Bucket: aws.String("non-existent-bucket"),
		})
		if err == nil {
			t.Fatal("Expected error for non-existent bucket, got nil")
		}
	})
}
