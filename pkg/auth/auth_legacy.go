package auth

import (
	"encoding/hex"
	"net/http"
	"strings"
)

func isPostFormUpload(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	ct := r.Header.Get("Content-Type")
	return strings.HasPrefix(ct, "multipart/form-data")
}

func (a *AWS4Authenticator) authenticatePostPolicy(r *http.Request) (string, error) {
	// Extract form fields
	key := r.FormValue("key")
	if key == "" {
		return "", NewAuthError("InvalidArgument", "Bucket POST must contain a field named 'key'.")
	}

	policy := r.FormValue("policy")
	if policy == "" {
		policy = r.FormValue("Policy")
	}

	algorithm := r.FormValue("X-Amz-Algorithm")
	if algorithm == "" {
		algorithm = r.FormValue("x-amz-algorithm")
	}
	if algorithm != "AWS4-HMAC-SHA256" {
		return "", NewAuthError("InvalidArgument", "Invalid or missing algorithm")
	}

	credential := r.FormValue("X-Amz-Credential")
	if credential == "" {
		credential = r.FormValue("x-amz-credential")
	}
	date := r.FormValue("X-Amz-Date")
	if date == "" {
		date = r.FormValue("x-amz-date")
	}
	signature := r.FormValue("X-Amz-Signature")
	if signature == "" {
		signature = r.FormValue("x-amz-signature")
	}

	if policy == "" || signature == "" || credential == "" || date == "" {
		return "", NewAuthError("InvalidArgument", "Missing required form fields")
	}

	// Parse credential: accessKeyID/date/region/service/aws4_request
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return "", NewAuthError("InvalidArgument", "Invalid credential format")
	}

	accessKeyID := credParts[0]
	credDate := credParts[1]
	region := credParts[2]
	service := credParts[3]

	secretAccessKey, exists := a.credentials[accessKeyID]
	if !exists {
		return "", NewAuthError("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records")
	}

	// The signature for POST is: hex(HMAC-SHA256(signingKey, policy))
	// where policy is the base64-encoded policy string
	signingKey := CalculateSigningKey(secretAccessKey, credDate, region, service)
	expectedSig := hex.EncodeToString(hmacSHA256(signingKey, []byte(policy)))

	if signature != expectedSig {
		return "", NewAuthError("XAmzContentSHA256Mismatch", "The request signature we calculated does not match the signature you provided")
	}

	return accessKeyID, nil
}
