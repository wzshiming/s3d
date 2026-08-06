package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/wzshiming/s3d/pkg/auth"
	"github.com/wzshiming/s3d/pkg/storage"
)

// setupAuthTestServer creates an S3 handler wrapped with the auth middleware
func setupAuthTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "s3d-anon-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	store, err := storage.NewStorage(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.CreateBucket("existing-bucket"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutObject("existing-bucket", "existing-key", strings.NewReader("data"), storage.Metadata{}, "", ""); err != nil {
		t.Fatal(err)
	}

	authenticator := auth.NewAWS4Authenticator()
	authenticator.AddCredentials("test-access-key", "test-secret-key")

	srv := httptest.NewServer(authenticator.AuthMiddleware(NewS3Handler(store)))
	t.Cleanup(srv.Close)
	return srv
}

func TestHealthEndpointsUnauthenticated(t *testing.T) {
	srv := setupAuthTestServer(t)

	for _, path := range []string{"/minio/health/live", "/minio/health/ready"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: expected 200, got %d", path, resp.StatusCode)
		}
	}
}

func TestAnonymousRequests(t *testing.T) {
	srv := setupAuthTestServer(t)

	tests := []struct {
		name         string
		method       string
		path         string
		expectedCode int
	}{
		{"list buckets denied", http.MethodGet, "/", http.StatusForbidden},
		{"get object in missing bucket", http.MethodGet, "/no-such-bucket/key", http.StatusNotFound},
		{"get missing object", http.MethodGet, "/existing-bucket/no-such-key", http.StatusNotFound},
		{"delete object in missing bucket", http.MethodDelete, "/no-such-bucket/key", http.StatusNotFound},
		{"delete missing object", http.MethodDelete, "/existing-bucket/no-such-key", http.StatusNotFound},
		{"get existing object denied", http.MethodGet, "/existing-bucket/existing-key", http.StatusForbidden},
		{"put object denied", http.MethodPut, "/existing-bucket/new-key", http.StatusForbidden},
		{"put object in missing bucket", http.MethodPut, "/no-such-bucket/new-key", http.StatusNotFound},
		{"list missing bucket", http.MethodGet, "/no-such-bucket", http.StatusNotFound},
		{"list existing bucket denied", http.MethodGet, "/existing-bucket", http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, srv.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tt.expectedCode {
				t.Errorf("%s %s: expected %d, got %d", tt.method, tt.path, tt.expectedCode, resp.StatusCode)
			}
		})
	}
}

func TestSignedRequestStillAuthenticated(t *testing.T) {
	srv := setupAuthTestServer(t)

	// A request with an invalid Authorization header must still be rejected
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/existing-bucket/existing-key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=bad/20230101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=bad")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for invalid signature, got %d", resp.StatusCode)
	}
}
