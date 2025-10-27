package integration

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestObjectOperations tests object-related S3 operations
func TestObjectOperations(t *testing.T) {
	bucketName := "test-object-operations"
	objectKey := "test-object.txt"
	objectContent := "Hello, S3! This is a test object."

	// Create bucket first
	_, err := ts.client.CreateBucket(ts.ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	// Test: Put object
	t.Run("PutObject", func(t *testing.T) {
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}
	})

	// Test: List objects with ListObjectsV2
	t.Run("ListObjectsV2", func(t *testing.T) {
		output, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				if *obj.Size != int64(len(objectContent)) {
					t.Errorf("Object size mismatch: got %d, want %d", *obj.Size, len(objectContent))
				}
				break
			}
		}

		if !found {
			t.Fatal("Object not found in ListObjectsV2")
		}

		if *output.KeyCount != int32(len(output.Contents)) {
			t.Errorf("KeyCount mismatch: got %d, want %d", *output.KeyCount, len(output.Contents))
		}
	})

	// Test: List objects with ListObjects (v1)
	t.Run("ListObjects", func(t *testing.T) {
		output, err := ts.client.ListObjects(ts.ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects (v1): %v", err)
		}

		found := false
		for _, obj := range output.Contents {
			if *obj.Key == objectKey {
				found = true
				if *obj.Size != int64(len(objectContent)) {
					t.Errorf("Object size mismatch: got %d, want %d", *obj.Size, len(objectContent))
				}
				break
			}
		}

		if !found {
			t.Fatal("Object not found in ListObjects (v1)")
		}
	})

	// Test: Get object
	t.Run("GetObject", func(t *testing.T) {
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
			t.Fatalf("Failed to read object body: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Object content mismatch: got %q, want %q", string(data), objectContent)
		}
	})

	// Test: Copy object
	copiedKey := "copied-object.txt"
	t.Run("CopyObject", func(t *testing.T) {
		_, err := ts.client.CopyObject(ts.ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(copiedKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, objectKey)),
		})
		if err != nil {
			t.Fatalf("Failed to copy object: %v", err)
		}

		// Verify copied object
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(copiedKey),
		})
		if err != nil {
			t.Fatalf("Failed to get copied object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read copied object body: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Copied object content mismatch: got %q, want %q", string(data), objectContent)
		}
	})

	// Test: Rename object
	renamedKey := "renamed-object.txt"
	t.Run("RenameObject", func(t *testing.T) {
		// Put a new object for renaming
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(objectContent),
		})
		if err != nil {
			t.Fatalf("Failed to put object for rename: %v", err)
		}

		// Rename the object
		_, err = ts.client.RenameObject(ts.ctx, &s3.RenameObjectInput{
			Bucket:       aws.String(bucketName),
			Key:          aws.String(renamedKey),
			RenameSource: aws.String(fmt.Sprintf("%s/%s", bucketName, objectKey)),
		})
		if err != nil {
			t.Fatalf("Failed to rename object: %v", err)
		}

		// Verify renamed object exists
		output, err := ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(renamedKey),
		})
		if err != nil {
			t.Fatalf("Failed to get renamed object: %v", err)
		}
		defer output.Body.Close()

		data, err := io.ReadAll(output.Body)
		if err != nil {
			t.Fatalf("Failed to read renamed object body: %v", err)
		}

		if string(data) != objectContent {
			t.Errorf("Renamed object content mismatch: got %q, want %q", string(data), objectContent)
		}

		// Verify original object no longer exists
		_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err == nil {
			t.Errorf("Expected error when getting original object after rename")
		}
	})

	// Test: Delete object
	t.Run("DeleteObject", func(t *testing.T) {
		// Delete the renamed object
		_, err := ts.client.DeleteObject(ts.ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(renamedKey),
		})
		if err != nil {
			t.Fatalf("Failed to delete object: %v", err)
		}

		// Verify object is deleted
		_, err = ts.client.GetObject(ts.ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(renamedKey),
		})
		if err == nil {
			t.Errorf("Expected error when getting deleted object")
		}
	})
}

// TestDeleteObjects tests the batch delete operation
func TestDeleteObjects(t *testing.T) {
	bucketName := "test-delete-objects"

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

	// Create test objects
	testObjects := []string{
		"obj1.txt",
		"obj2.txt",
		"obj3.txt",
		"obj4.txt",
		"obj5.txt",
		"folder/obj6.txt",
		"folder/obj7.txt",
	}

	for _, key := range testObjects {
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(fmt.Sprintf("Content of %s", key)),
		})
		if err != nil {
			t.Fatalf("Failed to put object %s: %v", key, err)
		}
	}

	// Test: Delete multiple objects
	t.Run("DeleteMultipleObjects", func(t *testing.T) {
		objectsToDelete := []types.ObjectIdentifier{
			{Key: aws.String("obj1.txt")},
			{Key: aws.String("obj2.txt")},
			{Key: aws.String("obj3.txt")},
		}

		output, err := ts.client.DeleteObjects(ts.ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects failed: %v", err)
		}

		// Verify deleted objects are returned
		if len(output.Deleted) != 3 {
			t.Errorf("Expected 3 deleted objects, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
			for _, e := range output.Errors {
				t.Logf("Error: %s - %s", *e.Key, *e.Message)
			}
		}

		// Verify objects are actually deleted
		for _, obj := range objectsToDelete {
			_, err := ts.client.HeadObject(ts.ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
			if err == nil {
				t.Errorf("Object %s should have been deleted", *obj.Key)
			}
		}

		// Verify remaining objects still exist
		output2, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		if *output2.KeyCount != 4 {
			t.Errorf("Expected 4 remaining objects, got %d", *output2.KeyCount)
		}
	})

	// Test: Delete with quiet mode
	t.Run("DeleteWithQuietMode", func(t *testing.T) {
		output, err := ts.client.DeleteObjects(ts.ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{Key: aws.String("obj4.txt")},
				},
				Quiet: aws.Bool(true),
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects with quiet mode failed: %v", err)
		}

		// In quiet mode, deleted objects should not be returned
		if len(output.Deleted) != 0 {
			t.Errorf("Expected 0 deleted objects in quiet mode, got %d", len(output.Deleted))
		}

		// Verify object is actually deleted
		_, err = ts.client.HeadObject(ts.ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("obj4.txt"),
		})
		if err == nil {
			t.Errorf("Object obj4.txt should have been deleted")
		}
	})

	// Test: Delete non-existent objects
	t.Run("DeleteNonexistentObjects", func(t *testing.T) {
		output, err := ts.client.DeleteObjects(ts.ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{Key: aws.String("nonexistent1.txt")},
					{Key: aws.String("nonexistent2.txt")},
				},
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects with nonexistent objects failed: %v", err)
		}

		// S3 treats deleting nonexistent objects as success
		if len(output.Deleted) != 2 {
			t.Errorf("Expected 2 deleted objects, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
		}
	})

	// Test: Delete objects with prefix (folder)
	t.Run("DeleteObjectsInFolder", func(t *testing.T) {
		output, err := ts.client.DeleteObjects(ts.ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{Key: aws.String("folder/obj6.txt")},
					{Key: aws.String("folder/obj7.txt")},
				},
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects in folder failed: %v", err)
		}

		if len(output.Deleted) != 2 {
			t.Errorf("Expected 2 deleted objects, got %d", len(output.Deleted))
		}

		// Verify objects are deleted
		list, err := ts.client.ListObjectsV2(ts.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String("folder/"),
		})
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		if *list.KeyCount != 0 {
			t.Errorf("Expected 0 objects in folder after deletion, got %d", *list.KeyCount)
		}
	})

	// Test: Delete mixed existing and non-existing objects
	t.Run("DeleteMixedObjects", func(t *testing.T) {
		// Put a test object first
		_, err := ts.client.PutObject(ts.ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("obj5.txt"),
			Body:   strings.NewReader("Content of obj5.txt"),
		})
		if err != nil {
			t.Fatalf("Failed to put object: %v", err)
		}

		output, err := ts.client.DeleteObjects(ts.ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{Key: aws.String("obj5.txt")},        // exists
					{Key: aws.String("nonexistent3.txt")}, // doesn't exist
				},
			},
		})
		if err != nil {
			t.Fatalf("DeleteObjects with mixed objects failed: %v", err)
		}

		// Both should be in deleted list
		if len(output.Deleted) != 2 {
			t.Errorf("Expected 2 deleted objects, got %d", len(output.Deleted))
		}

		// Verify no errors
		if len(output.Errors) != 0 {
			t.Errorf("Expected no errors, got %d", len(output.Errors))
		}
	})

	// Test: Delete with invalid bucket
	t.Run("DeleteObjectsInvalidBucket", func(t *testing.T) {
		_, err := ts.client.DeleteObjects(ts.ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String("nonexistent-bucket-12345"),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{Key: aws.String("obj1.txt")},
				},
			},
		})
		if err == nil {
			t.Fatal("Expected error for nonexistent bucket, got nil")
		}
	})
}
