package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/wzshiming/s3d/pkg/auth"
	"github.com/wzshiming/s3d/pkg/storage"
)

// handlePresignedPost handles presigned POST requests for browser-based uploads
func (s *S3Handler) handlePresignedPost(w http.ResponseWriter, r *http.Request, bucket string, authenticator *auth.AWS4Authenticator) {
	// Parse multipart form (max 32MB in memory)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		s.errorResponse(w, r, "InvalidArgument", "Failed to parse multipart form", http.StatusBadRequest)
		return
	}

	// Extract form values
	formValues := make(map[string]string)
	for key, values := range r.MultipartForm.Value {
		if len(values) > 0 {
			formValues[key] = values[0]
		}
	}

	// Get the file from form
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		s.errorResponse(w, r, "InvalidArgument", "File field is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Validate presigned POST policy and signature
	if authenticator != nil {
		if err := authenticator.ValidatePresignedPost(formValues, fileHeader.Size, bucket); err != nil {
			var authErr *auth.AuthError
			if e, ok := err.(*auth.AuthError); ok {
				authErr = e
			} else {
				authErr = auth.NewAuthError("AccessDenied", err.Error())
			}
			s.errorResponse(w, r, authErr.Code, authErr.Message, http.StatusForbidden)
			return
		}
	}

	// Extract key from form values
	key := formValues["key"]
	if key == "" {
		s.errorResponse(w, r, "InvalidArgument", "key field is required", http.StatusBadRequest)
		return
	}

	// Build metadata from form values
	metadata := storage.Metadata{}

	// Map common form fields to metadata
	if contentType := formValues["Content-Type"]; contentType != "" {
		metadata.ContentType = contentType
	}
	if cacheControl := formValues["Cache-Control"]; cacheControl != "" {
		metadata.CacheControl = cacheControl
	}
	if contentDisposition := formValues["Content-Disposition"]; contentDisposition != "" {
		metadata.ContentDisposition = contentDisposition
	}

	// Extract x-amz-meta-* fields
	for formKey, formValue := range formValues {
		lowerKey := strings.ToLower(formKey)
		if strings.HasPrefix(lowerKey, "x-amz-meta-") {
			if metadata.XAmzMeta == nil {
				metadata.XAmzMeta = make(map[string]string)
			}
			metaKey := lowerKey[len("x-amz-meta-"):]
			metadata.XAmzMeta[metaKey] = formValue
		}
	}

	// Store the object
	objInfo, err := s.storage.PutObject(bucket, key, file, metadata, "")
	if err != nil {
		switch err {
		case storage.ErrBucketNotFound:
			s.errorResponse(w, r, "NoSuchBucket", "Bucket does not exist", http.StatusNotFound)
		default:
			s.errorResponse(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Check if client expects redirect
	successActionRedirect := formValues["success_action_redirect"]
	if successActionRedirect != "" {
		// Redirect to success URL
		redirectURL := fmt.Sprintf("%s?bucket=%s&key=%s&etag=%s", successActionRedirect, bucket, key, objInfo.ETag)
		http.Redirect(w, r, redirectURL, http.StatusSeeOther)
		return
	}

	// Check for success_action_status
	successActionStatus := formValues["success_action_status"]
	switch successActionStatus {
	case "200":
		// Return 200 OK with empty body
		s.setHeaders(w, r)
		w.Header().Set("ETag", fmt.Sprintf("%q", objInfo.ETag))
		w.WriteHeader(http.StatusOK)
		return

	case "201":
		// Return 201 Created with XML response (AWS S3 default behavior)
		fallthrough

	default:
		// Default to 204 No Content
		if successActionStatus == "" || successActionStatus == "204" {
			s.setHeaders(w, r)
			w.Header().Set("ETag", fmt.Sprintf("%q", objInfo.ETag))
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// For 201, return XML response with object details
		result := PostObjectResult{
			Location: fmt.Sprintf("http://%s/%s/%s", r.Host, bucket, key),
			Bucket:   bucket,
			Key:      key,
			ETag:     fmt.Sprintf("%q", objInfo.ETag),
		}
		s.xmlResponse(w, r, result, http.StatusCreated)
	}
}

// IsPresignedPost checks if the request is a presigned POST request
func IsPresignedPost(r *http.Request) bool {
	// Must be POST method
	if r.Method != http.MethodPost {
		return false
	}

	// Must have multipart/form-data content type
	contentType := r.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "multipart/form-data") {
		return false
	}

	// Check if it's not a multipart upload operation
	query := r.URL.Query()
	if query.Has("uploads") || query.Has("uploadId") {
		return false
	}

	return true
}

// extractPresignedPostBucket extracts the bucket name for presigned POST
// The bucket can be in the path (/bucket) or in the form field
func extractPresignedPostBucket(r *http.Request, path string) (string, error) {
	// First try to get bucket from path
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
	if len(parts) > 0 && parts[0] != "" {
		return parts[0], nil
	}

	// If not in path, we'll need to parse form to get it
	// This is handled by the caller
	return "", fmt.Errorf("bucket not found in path")
}

// readFormValue safely reads a form value after ParseMultipartForm
func readFormValue(r *http.Request, key string) string {
	if r.MultipartForm != nil && r.MultipartForm.Value != nil {
		if values, ok := r.MultipartForm.Value[key]; ok && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

// readFormFile safely reads a file from multipart form
func readFormFile(r *http.Request, key string) (io.ReadCloser, int64, error) {
	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		if fileHeaders, ok := r.MultipartForm.File[key]; ok && len(fileHeaders) > 0 {
			fileHeader := fileHeaders[0]
			file, err := fileHeader.Open()
			if err != nil {
				return nil, 0, err
			}
			return file, fileHeader.Size, nil
		}
	}
	return nil, 0, fmt.Errorf("file not found")
}
