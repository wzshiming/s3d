package server

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/wzshiming/s3d/pkg/storage"
)

// handlePostObject handles POST object upload (presigned POST / form-based upload)
func (s *S3Handler) handlePostObject(w http.ResponseWriter, r *http.Request, bucket string) {
	// Extract form fields
	key := r.FormValue("key")
	successActionStatus := r.FormValue("success_action_status")
	successActionRedirect := r.FormValue("success_action_redirect")

	policy := r.FormValue("policy")
	if policy == "" {
		policy = r.FormValue("Policy")
	}

	if policy != "" {
		// Validate policy conditions (expiration, bucket, key, content-length, etc.)
		policyBytes, err := base64.StdEncoding.DecodeString(policy)
		if err != nil {
			s.response(w, r, "InvalidArgument", fmt.Sprintf("Invalid Policy: base64 decoding failed: %v", err), http.StatusBadRequest)
			return
		}

		var policyDoc postPolicy
		if err := json.Unmarshal(policyBytes, &policyDoc); err != nil {
			s.response(w, r, "InvalidArgument", fmt.Sprintf("Invalid Policy: JSON parsing failed: %v, %q", err, policy), http.StatusBadRequest)
			return
		}

		if err := validatePostPolicy(r, policyDoc, bucket, key); err != nil {
			s.response(w, r, "Denied", err.Error(), http.StatusForbidden)
			return
		}
	}

	// Handle ${filename} substitution in key
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		s.response(w, r, "InvalidArgument", "Bucket POST must contain a field named 'file'.", http.StatusBadRequest)
		return
	}
	defer file.Close()

	if strings.Contains(key, "${filename}") {
		key = strings.ReplaceAll(key, "${filename}", fileHeader.Filename)
	}

	// Build metadata from form fields
	var metadata storage.Metadata
	if ct := r.FormValue("Content-Type"); ct != "" {
		metadata.ContentType = ct
	}
	if cd := r.FormValue("Content-Disposition"); cd != "" {
		metadata.ContentDisposition = cd
	}
	if cc := r.FormValue("Cache-Control"); cc != "" {
		metadata.CacheControl = cc
	}
	for k, vs := range r.MultipartForm.Value {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(vs) > 0 {
			metaKey := lower[len("x-amz-meta-"):]
			if metadata.XAmzMeta == nil {
				metadata.XAmzMeta = make(map[string]string)
			}
			metadata.XAmzMeta[metaKey] = vs[0]
		}
	}

	// Parse tagging form field (a Tagging XML document)
	if taggingValue := r.FormValue("tagging"); taggingValue != "" {
		var tagging Tagging
		if err := xml.Unmarshal([]byte(taggingValue), &tagging); err != nil {
			s.response(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
			return
		}
		tags := tagsFromXML(tagging)
		if err := validateTags(tags); err != nil {
			s.response(w, r, "InvalidTag", err.Error(), http.StatusBadRequest)
			return
		}
		sortTags(tags)
		metadata.Tagging = tags
	}

	objInfo, err := s.storage.PutObject(bucket, key, file, metadata, "", "")
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	// Handle success_action_redirect
	if successActionRedirect != "" {
		redirectURL := fmt.Sprintf("%s?bucket=%s&key=%s&etag=%s", successActionRedirect, bucket, key, objInfo.ETag)
		http.Redirect(w, r, redirectURL, http.StatusSeeOther)
		return
	}

	// Handle success_action_status
	statusCode := http.StatusNoContent
	if successActionStatus == "200" {
		statusCode = http.StatusOK
	} else if successActionStatus == "201" {
		statusCode = http.StatusCreated
	}

	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", objInfo.ETag))

	if statusCode == http.StatusCreated {
		// Return XML response for 201
		result := PostObjectResult{
			Location: fmt.Sprintf("/%s/%s", bucket, key),
			Bucket:   bucket,
			Key:      key,
			ETag:     fmt.Sprintf("%q", objInfo.ETag),
		}
		s.xmlResponse(w, r, result, http.StatusCreated)
		return
	}

	w.WriteHeader(statusCode)
}

// postPolicy represents a parsed POST policy document
type postPolicy struct {
	Expiration string            `json:"expiration"`
	Conditions []json.RawMessage `json:"conditions"`
}

// validatePostPolicy validates POST policy conditions against the request
func validatePostPolicy(r *http.Request, policyDoc postPolicy, bucket, key string) error {
	// Check expiration
	if policyDoc.Expiration != "" {
		expTime, err := parsePostPolicyExpiration(policyDoc.Expiration)
		if err != nil {
			return fmt.Errorf("invalid expiration: %v", err)
		}
		if time.Now().After(expTime) {
			return fmt.Errorf("Invalid according to Policy: Policy expired")
		}
	}

	// Check conditions
	for _, rawCond := range policyDoc.Conditions {
		// Conditions can be:
		// 1. {"key": "value"} - exact match
		// 2. ["eq", "$key", "value"] - exact match
		// 3. ["starts-with", "$key", "value"] - prefix match
		// 4. ["content-length-range", min, max] - content length range

		// Try array form first
		var arrCond []json.RawMessage
		if err := json.Unmarshal(rawCond, &arrCond); err == nil && len(arrCond) >= 2 {
			var op string
			if err := json.Unmarshal(arrCond[0], &op); err == nil {
				opLower := strings.ToLower(op)
				if opLower == "content-length-range" && len(arrCond) == 3 {
					var minLen, maxLen int64
					if err := json.Unmarshal(arrCond[1], &minLen); err != nil {
						continue
					}
					if err := json.Unmarshal(arrCond[2], &maxLen); err != nil {
						continue
					}
					if r.ContentLength > 0 && (r.ContentLength < minLen || r.ContentLength > maxLen) {
						return fmt.Errorf("Invalid according to Policy: Policy Condition failed: content-length-range")
					}
					continue
				}

				if (opLower == "eq" || opLower == "starts-with") && len(arrCond) == 3 {
					var fieldName, expected string
					if err := json.Unmarshal(arrCond[1], &fieldName); err != nil {
						continue
					}
					if err := json.Unmarshal(arrCond[2], &expected); err != nil {
						continue
					}

					actual := getPostFieldValue(r, bucket, key, fieldName)
					if opLower == "eq" {
						if actual != expected {
							return fmt.Errorf("Invalid according to Policy: Policy Condition failed: [eq, %s, %s]", fieldName, expected)
						}
					} else {
						if !strings.HasPrefix(actual, expected) {
							return fmt.Errorf("Invalid according to Policy: Policy Condition failed: [starts-with, %s, %s]", fieldName, expected)
						}
					}
					continue
				}
			}
			continue
		}

		// Try object form: {"key": "value"}
		var mapCond map[string]string
		if err := json.Unmarshal(rawCond, &mapCond); err == nil {
			for fieldName, expected := range mapCond {
				actual := getPostFieldValue(r, bucket, key, "$"+fieldName)
				if actual != expected {
					return fmt.Errorf("Invalid according to Policy: Policy Condition failed: {%s: %s}", fieldName, expected)
				}
			}
		}
	}

	return nil
}

// getPostFieldValue returns the value of a field for POST policy validation
func getPostFieldValue(r *http.Request, bucket, key, fieldName string) string {
	if !strings.HasPrefix(fieldName, "$") {
		return ""
	}
	name := fieldName[1:] // remove leading $
	nameLower := strings.ToLower(name)
	switch nameLower {
	case "bucket":
		return bucket
	case "key":
		return key
	default:
		return r.FormValue(name)
	}
}

// parsePostPolicyExpiration parses the expiration timestamp from a POST policy
func parsePostPolicyExpiration(expiration string) (t time.Time, err error) {
	// Try ISO 8601 format used by AWS
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.999Z",
		time.RFC3339,
	}
	for _, format := range formats {
		t, err = time.Parse(format, expiration)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported expiration format: %s", expiration)
}
