// Package auth implements AWS Signature V4 authentication for S3-compatible servers.
//
// The package provides authentication mechanisms including credential management,
// signature validation, and HTTP middleware integration.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"
)

// Credentials represents AWS credentials
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// AWS4Authenticator handles authentication
type AWS4Authenticator struct {
	credentials map[string]string // accessKeyID -> secretAccessKey
}

// NewAWS4Authenticator creates a new authenticator
func NewAWS4Authenticator() *AWS4Authenticator {
	return &AWS4Authenticator{
		credentials: make(map[string]string),
	}
}

// AddCredentials adds credentials for authentication
func (a *AWS4Authenticator) AddCredentials(accessKeyID, secretAccessKey string) {
	a.credentials[accessKeyID] = secretAccessKey
}

// AuthMiddleware is HTTP middleware for authentication
func (a *AWS4Authenticator) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := a.authenticate(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("Authentication failed: %v", err), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Authenticate validates the request signature
func (a *AWS4Authenticator) authenticate(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")

	// Check if authentication is required
	if authHeader == "" {
		return "", fmt.Errorf("missing authorization header")
	}

	// Parse AWS Signature Version 4
	if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		return a.authenticateV4(r, authHeader)
	}

	return "", fmt.Errorf("unsupported authorization type")
}

// authenticateV4 validates AWS Signature Version 4
func (a *AWS4Authenticator) authenticateV4(r *http.Request, authHeader string) (string, error) {
	// Parse authorization header
	// Format: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return "", fmt.Errorf("invalid authorization header format")
	}

	authParams := strings.TrimPrefix(authHeader, "AWS4-HMAC-SHA256 ")
	params := make(map[string]string)
	for _, part := range strings.Split(authParams, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			params[kv[0]] = kv[1]
		}
	}

	credential := params["Credential"]
	signature := params["Signature"]
	signedHeaders := params["SignedHeaders"]

	if credential == "" || signature == "" || signedHeaders == "" {
		return "", fmt.Errorf("missing required authorization parameters")
	}

	// Parse credential
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return "", fmt.Errorf("invalid credential format")
	}

	accessKeyID := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	// Check if credentials exist
	secretAccessKey, exists := a.credentials[accessKeyID]
	if !exists {
		return "", fmt.Errorf("invalid access key")
	}

	// Calculate expected signature
	expectedSignature, err := a.calculateSignatureV4(r, secretAccessKey, date, region, service, signedHeaders)
	if err != nil {
		return "", err
	}

	if signature != expectedSignature {
		return "", fmt.Errorf("signature does not match")
	}

	return accessKeyID, nil
}

// calculateSignatureV4 calculates AWS Signature Version 4
func (a *AWS4Authenticator) calculateSignatureV4(r *http.Request, secretAccessKey, date, region, service, signedHeaders string) (string, error) {
	// Step 1: Create canonical request
	canonicalRequest := a.createCanonicalRequest(r, signedHeaders)

	// Step 2: Create string to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)

	hashedCanonicalRequest := sha256Hash(canonicalRequest)

	timestamp := r.Header.Get("X-Amz-Date")
	if timestamp == "" {
		timestamp = r.Header.Get("Date")
	}

	stringToSign := strings.Join([]string{
		algorithm,
		timestamp,
		credentialScope,
		hashedCanonicalRequest,
	}, "\n")

	// Step 3: Calculate signature
	dateKey := hmacSHA256([]byte("AWS4"+secretAccessKey), []byte(date))
	dateRegionKey := hmacSHA256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSHA256(dateRegionKey, []byte(service))
	signingKey := hmacSHA256(dateRegionServiceKey, []byte("aws4_request"))

	signature := hmacSHA256(signingKey, []byte(stringToSign))

	return hex.EncodeToString(signature), nil
}

// canonicalURI encodes the URI path according to AWS Signature V4 specification
// Each path segment is URI-encoded, but the forward slashes are preserved
func (a *AWS4Authenticator) canonicalURI(path string) string {
	if path == "" {
		return "/"
	}

	// Split path into segments and encode each one
	segments := strings.Split(path, "/")
	var encoded []string
	for _, segment := range segments {
		if segment == "" {
			encoded = append(encoded, "")
		} else {
			// URI-encode each segment according to RFC 3986 for AWS SigV4
			encoded = append(encoded, uriEncode(segment))
		}
	}
	return strings.Join(encoded, "/")
}

// uriEncode performs URI encoding according to RFC 3986 for AWS Signature V4
// Only unreserved characters (A-Z, a-z, 0-9, '-', '_', '.', '~') are not encoded
func uriEncode(s string) string {
	var result strings.Builder
	var buf [4]byte // Buffer for UTF-8 encoding (max 4 bytes per rune)
	
	// Iterate over runes to properly handle multi-byte UTF-8 characters
	for _, r := range s {
		// Check if character is unreserved (safe to keep unencoded)
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.' || r == '~' {
			result.WriteRune(r)
		} else {
			// Encode the rune as UTF-8 bytes and percent-encode each byte
			n := utf8.EncodeRune(buf[:], r)
			for i := 0; i < n; i++ {
				result.WriteString(fmt.Sprintf("%%%02X", buf[i]))
			}
		}
	}
	return result.String()
}

// createCanonicalRequest creates a canonical request for AWS Signature V4
func (a *AWS4Authenticator) createCanonicalRequest(r *http.Request, signedHeaders string) string {
	// Method
	method := r.Method

	// URI - use EscapedPath to get the properly encoded path, then encode each segment
	// Note: r.URL.Path contains the decoded path, so we need to re-encode it
	uri := a.canonicalURI(r.URL.Path)

	// Query string
	queryString := r.URL.Query()
	var queryParams []string
	for key := range queryString {
		for _, value := range queryString[key] {
			// AWS SigV4 requires URL encoding of query parameters
			encodedKey := url.QueryEscape(key)
			encodedValue := url.QueryEscape(value)
			queryParams = append(queryParams, fmt.Sprintf("%s=%s", encodedKey, encodedValue))
		}
	}
	sort.Strings(queryParams)
	canonicalQueryString := strings.Join(queryParams, "&")

	// Headers
	headersList := strings.Split(signedHeaders, ";")
	var canonicalHeaders []string
	for _, header := range headersList {
		var value string
		if strings.ToLower(header) == "host" {
			// Host header is special in Go and stored in r.Host
			value = r.Host
		} else {
			value = r.Header.Get(header)
		}
		canonicalHeaders = append(canonicalHeaders, fmt.Sprintf("%s:%s\n", strings.ToLower(header), strings.TrimSpace(value)))
	}
	sort.Strings(canonicalHeaders)
	canonicalHeadersString := strings.Join(canonicalHeaders, "")

	// Payload hash
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	return strings.Join([]string{
		method,
		uri,
		canonicalQueryString,
		canonicalHeadersString,
		signedHeaders,
		payloadHash,
	}, "\n")
}

// sha256Hash calculates SHA256 hash
func sha256Hash(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// hmacSHA256 calculates HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
