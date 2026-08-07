package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestMatchETag(t *testing.T) {
	const etag = "37b51d194a7513e45b56f6524f2d51f2"

	tests := []struct {
		name        string
		headerValue string
		weak        bool
		want        bool
	}{
		{name: "quoted match", headerValue: `"` + etag + `"`, want: true},
		{name: "unquoted match", headerValue: etag, want: true},
		{name: "mismatch", headerValue: `"ABCORZ"`, want: false},
		{name: "star", headerValue: "*", want: true},
		{name: "list with match", headerValue: `"ABCORZ", "` + etag + `"`, want: true},
		{name: "list without match", headerValue: `"ABCORZ", "ZZZ"`, want: false},
		{name: "weak tag strong comparison", headerValue: `W/"` + etag + `"`, weak: false, want: false},
		{name: "weak tag weak comparison", headerValue: `W/"` + etag + `"`, weak: true, want: true},
		{name: "empty", headerValue: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchETag(tt.headerValue, etag, tt.weak); got != tt.want {
				t.Errorf("matchETag(%q, %q, %v) = %v, want %v", tt.headerValue, etag, tt.weak, got, tt.want)
			}
		})
	}
}

func TestParseRangeHeader(t *testing.T) {
	tests := []struct {
		name            string
		header          string
		size            int64
		wantValid       bool
		wantSatisfiable bool
	}{
		{name: "first bytes", header: "bytes=0-1", size: 3, wantValid: true, wantSatisfiable: true},
		{name: "open ended", header: "bytes=1-", size: 3, wantValid: true, wantSatisfiable: true},
		{name: "suffix", header: "bytes=-2", size: 3, wantValid: true, wantSatisfiable: true},
		{name: "beyond size", header: "bytes=40-50", size: 3, wantValid: true, wantSatisfiable: false},
		{name: "beyond size open ended", header: "bytes=40-", size: 3, wantValid: true, wantSatisfiable: false},
		{name: "empty object", header: "bytes=0-", size: 0, wantValid: true, wantSatisfiable: false},
		{name: "empty object suffix", header: "bytes=-2", size: 0, wantValid: true, wantSatisfiable: false},
		{name: "zero suffix", header: "bytes=-0", size: 3, wantValid: true, wantSatisfiable: false},
		{name: "multiple one satisfiable", header: "bytes=40-50, 0-1", size: 3, wantValid: true, wantSatisfiable: true},
		{name: "multiple none satisfiable", header: "bytes=40-50, 60-70", size: 3, wantValid: true, wantSatisfiable: false},
		{name: "last before first", header: "bytes=5-2", size: 10, wantValid: false},
		{name: "not bytes unit", header: "items=0-1", size: 3, wantValid: false},
		{name: "no dash", header: "bytes=1", size: 3, wantValid: false},
		{name: "non numeric", header: "bytes=abc", size: 3, wantValid: false},
		{name: "empty spec list", header: "bytes=", size: 3, wantValid: false},
		{name: "negative start", header: "bytes=--5", size: 3, wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, satisfiable := parseRangeHeader(tt.header, tt.size)
			if valid != tt.wantValid {
				t.Errorf("parseRangeHeader(%q, %d) valid = %v, want %v", tt.header, tt.size, valid, tt.wantValid)
			}
			if valid && satisfiable != tt.wantSatisfiable {
				t.Errorf("parseRangeHeader(%q, %d) satisfiable = %v, want %v", tt.header, tt.size, satisfiable, tt.wantSatisfiable)
			}
		})
	}
}

func TestGetObjectConditional(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-conditional-bucket"
	objectKey := "conditional.txt"
	objectContent := "bar"

	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	putOutput, err := ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(objectContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	etag := aws.ToString(putOutput.ETag)

	objectURL := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, objectKey)

	get := func(t *testing.T, headers map[string]string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, objectURL, nil)
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	t.Run("IfMatchGood", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Match": etag})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != objectContent {
			t.Errorf("Expected body %q, got %q", objectContent, string(body))
		}
	})

	t.Run("IfMatchStar", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Match": "*"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("IfMatchFailed", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Match": `"ABCORZ"`})
		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Fatalf("Expected 412, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "<Code>PreconditionFailed</Code>") {
			t.Errorf("Expected PreconditionFailed error body, got %q", string(body))
		}
		if resp.Header.Get("x-amz-checksum-sha256") != "" || resp.Header.Get("x-amz-checksum-md5") != "" {
			t.Error("Expected no checksum headers on error response")
		}
	})

	t.Run("IfNoneMatchGood", func(t *testing.T) {
		resp := get(t, map[string]string{"If-None-Match": etag})
		if resp.StatusCode != http.StatusNotModified {
			t.Fatalf("Expected 304, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if len(body) != 0 {
			t.Errorf("Expected empty body on 304, got %q", string(body))
		}
		if resp.Header.Get("ETag") != etag {
			t.Errorf("Expected ETag %q, got %q", etag, resp.Header.Get("ETag"))
		}
		if resp.Header.Get("x-amz-checksum-sha256") != "" || resp.Header.Get("x-amz-checksum-md5") != "" {
			t.Error("Expected no checksum headers on 304 response")
		}
	})

	t.Run("IfNoneMatchFailed", func(t *testing.T) {
		resp := get(t, map[string]string{"If-None-Match": "ABCORZ"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != objectContent {
			t.Errorf("Expected body %q, got %q", objectContent, string(body))
		}
	})

	t.Run("IfModifiedSincePast", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Modified-Since": "Mon, 01 Jan 2001 00:00:00 GMT"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("IfModifiedSinceFuture", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Modified-Since": "Sat, 01 Jan 2101 00:00:00 GMT"})
		if resp.StatusCode != http.StatusNotModified {
			t.Fatalf("Expected 304, got %d", resp.StatusCode)
		}
	})

	t.Run("IfUnmodifiedSincePast", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Unmodified-Since": "Mon, 01 Jan 2001 00:00:00 GMT"})
		if resp.StatusCode != http.StatusPreconditionFailed {
			t.Fatalf("Expected 412, got %d", resp.StatusCode)
		}
	})

	t.Run("IfUnmodifiedSinceFuture", func(t *testing.T) {
		resp := get(t, map[string]string{"If-Unmodified-Since": "Sat, 01 Jan 2101 00:00:00 GMT"})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
	})
}

func TestGetObjectInvalidRange(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-invalid-range-bucket"

	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	for key, content := range map[string]string{
		"content.txt": "testcontent",
		"empty.txt":   "",
	} {
		_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
	}

	get := func(t *testing.T, key, rangeHeader string) *http.Response {
		t.Helper()
		url := fmt.Sprintf("http://%s/%s/%s", ts.listener.Addr().String(), bucketName, key)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("NewRequest failed: %v", err)
		}
		req.Header.Set("Range", rangeHeader)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	t.Run("UnsatisfiableRange", func(t *testing.T) {
		resp := get(t, "content.txt", "bytes=40-50")
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("Expected 416, got %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Range"); got != "bytes */11" {
			t.Errorf("Expected Content-Range %q, got %q", "bytes */11", got)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "<Code>InvalidRange</Code>") {
			t.Errorf("Expected InvalidRange error body, got %q", string(body))
		}
		if resp.Header.Get("x-amz-checksum-sha256") != "" || resp.Header.Get("x-amz-checksum-md5") != "" {
			t.Error("Expected no checksum headers on error response")
		}
	})

	t.Run("EmptyObjectRange", func(t *testing.T) {
		resp := get(t, "empty.txt", "bytes=40-50")
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("Expected 416, got %d", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Range"); got != "bytes */0" {
			t.Errorf("Expected Content-Range %q, got %q", "bytes */0", got)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "<Code>InvalidRange</Code>") {
			t.Errorf("Expected InvalidRange error body, got %q", string(body))
		}
	})

	t.Run("SatisfiableRange", func(t *testing.T) {
		resp := get(t, "content.txt", "bytes=4-7")
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("Expected 206, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "cont" {
			t.Errorf("Expected body %q, got %q", "cont", string(body))
		}
		if got := resp.Header.Get("Content-Range"); got != "bytes 4-7/11" {
			t.Errorf("Expected Content-Range %q, got %q", "bytes 4-7/11", got)
		}
	})

	t.Run("SuffixRange", func(t *testing.T) {
		resp := get(t, "content.txt", "bytes=-7")
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("Expected 206, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "content" {
			t.Errorf("Expected body %q, got %q", "content", string(body))
		}
	})

	t.Run("MalformedRangeIgnored", func(t *testing.T) {
		resp := get(t, "content.txt", "bytes=abc")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "testcontent" {
			t.Errorf("Expected full body %q, got %q", "testcontent", string(body))
		}
	})
}

func TestCopyObjectToItself(t *testing.T) {
	ctx := context.Background()
	bucketName := "test-copy-to-itself-bucket"
	objectKey := "self.txt"
	objectContent := "self copy content"

	_, err := ts.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	_, err = ts.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(objectContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	copySource := fmt.Sprintf("%s/%s", bucketName, objectKey)

	t.Run("WithoutDirectiveFails", func(t *testing.T) {
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			CopySource: aws.String(copySource),
		})
		if err == nil {
			t.Fatal("Expected error when copying object to itself without REPLACE directive")
		}
		if !strings.Contains(err.Error(), "InvalidRequest") {
			t.Errorf("Expected InvalidRequest error, got %v", err)
		}
	})

	t.Run("WithReplaceDirectiveSucceeds", func(t *testing.T) {
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(bucketName),
			Key:               aws.String(objectKey),
			CopySource:        aws.String(copySource),
			MetadataDirective: types.MetadataDirectiveReplace,
			Metadata: map[string]string{
				"updated": "true",
			},
		})
		if err != nil {
			t.Fatalf("CopyObject to itself with REPLACE directive failed: %v", err)
		}

		output, err := ts.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}
		if output.Metadata["updated"] != "true" {
			t.Errorf("Expected metadata updated='true', got %q", output.Metadata["updated"])
		}
	})

	t.Run("CopyToOtherKeySucceeds", func(t *testing.T) {
		_, err := ts.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String("other.txt"),
			CopySource: aws.String(copySource),
		})
		if err != nil {
			t.Fatalf("CopyObject to different key failed: %v", err)
		}
	})
}
