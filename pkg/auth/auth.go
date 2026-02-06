// Package auth implements AWS Signature V4 authentication for S3-compatible servers.
//
// The package provides authentication mechanisms including credential management,
// signature validation, and HTTP middleware integration.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
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
			// Use specific error code if AuthError is returned
			var authErr *AuthError
			var errResp Error
			if errors.As(err, &authErr) {
				errResp = Error{
					Code:    authErr.Code,
					Message: authErr.Message,
				}
			} else {
				// Default to AccessDenied for other errors
				errResp = Error{
					Code:    "AccessDenied",
					Message: "Access Denied",
				}
			}

			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)

			if _, writeErr := w.Write([]byte(xml.Header)); writeErr != nil {
				return
			}
			if encodeErr := xml.NewEncoder(w).Encode(errResp); encodeErr != nil {
				return
			}
			return
		}

		// Wrap chunked upload requests with signature-validating reader
		if IsChunkedUpload(r) {
			wrappedReq, wrapErr := a.WrapChunkedRequest(r)
			if wrapErr != nil {
				var authErr *AuthError
				var errResp Error
				if errors.As(wrapErr, &authErr) {
					errResp = Error{
						Code:    authErr.Code,
						Message: authErr.Message,
					}
				} else {
					errResp = Error{
						Code:    "AccessDenied",
						Message: "Access Denied",
					}
				}

				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusForbidden)

				if _, writeErr := w.Write([]byte(xml.Header)); writeErr != nil {
					return
				}
				if encodeErr := xml.NewEncoder(w).Encode(errResp); encodeErr != nil {
					return
				}
				return
			}
			r = wrappedReq
		}

		next.ServeHTTP(w, r)
	})
}

// Authenticate validates the request signature
func (a *AWS4Authenticator) authenticate(r *http.Request) (string, error) {
	// Check for query string authentication (presigned URLs)
	queryParams := r.URL.Query()
	if queryParams.Get("X-Amz-Algorithm") != "" {
		return a.authenticateV4Query(r)
	}

	// Check if authentication is required
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
			return a.authenticateV4Header(r, authHeader)
		}
		return "", NewAuthError("InvalidArgument", "Unsupported authorization type")
	}

	return "", NewAuthError("AccessDenied", "Missing or invalid authentication information")
}

// authenticateV4Query validates AWS Signature Version 4 query string authentication
func (a *AWS4Authenticator) authenticateV4Query(r *http.Request) (string, error) {
	queryParams := r.URL.Query()

	// Parse required query parameters
	algorithm := queryParams.Get("X-Amz-Algorithm")
	credential := queryParams.Get("X-Amz-Credential")
	date := queryParams.Get("X-Amz-Date")
	expires := queryParams.Get("X-Amz-Expires")
	signedHeaders := queryParams.Get("X-Amz-SignedHeaders")
	signature := queryParams.Get("X-Amz-Signature")

	// Validate required parameters
	if algorithm != "AWS4-HMAC-SHA256" {
		return "", NewAuthError("InvalidArgument", "Invalid or missing algorithm")
	}
	if credential == "" || date == "" || signedHeaders == "" || signature == "" {
		return "", NewAuthError("InvalidArgument", "Missing required query parameters")
	}

	// Parse credential
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return "", NewAuthError("InvalidArgument", "Invalid credential format")
	}

	accessKeyID := credParts[0]
	credDate := credParts[1]
	region := credParts[2]
	service := credParts[3]

	// Check if credentials exist
	secretAccessKey, exists := a.credentials[accessKeyID]
	if !exists {
		return "", NewAuthError("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records")
	}

	// Validate expiration if provided
	if expires != "" {
		// Parse the X-Amz-Date timestamp (format: 20230101T000000Z)
		requestTime, err := parseAmzDate(date)
		if err != nil {
			return "", NewAuthError("InvalidArgument", fmt.Sprintf("Invalid X-Amz-Date format: %v", err))
		}

		// Parse expires duration (in seconds)
		expiresSeconds, err := strconv.Atoi(expires)
		if err != nil {
			return "", NewAuthError("InvalidArgument", fmt.Sprintf("Invalid X-Amz-Expires value: %v", err))
		}

		// Check if URL has expired
		expirationTime := requestTime.Add(time.Duration(expiresSeconds) * time.Second)
		if time.Now().After(expirationTime) {
			return "", NewAuthError("AccessDenied", "Presigned URL has expired")
		}
	}

	// Calculate expected signature
	expectedSignature, err := a.calculateSignatureV4Query(r, secretAccessKey, credDate, region, service, signedHeaders)
	if err != nil {
		return "", err
	}

	if signature != expectedSignature {
		return "", NewAuthError("XAmzContentSHA256Mismatch", "The request signature we calculated does not match the signature you provided")
	}

	return accessKeyID, nil
}

// authenticateV4Header validates AWS Signature Version 4
func (a *AWS4Authenticator) authenticateV4Header(r *http.Request, authHeader string) (string, error) {
	// Parse authorization header
	// Format: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return "", NewAuthError("InvalidArgument", "Invalid authorization header format")
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
		return "", NewAuthError("InvalidArgument", "Missing required authorization parameters")
	}

	// Parse credential
	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return "", NewAuthError("InvalidArgument", "Invalid credential format")
	}

	accessKeyID := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	// Check if credentials exist
	secretAccessKey, exists := a.credentials[accessKeyID]
	if !exists {
		return "", NewAuthError("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records")
	}

	// Calculate expected signature
	expectedSignature, err := a.calculateSignatureV4Header(r, secretAccessKey, date, region, service, signedHeaders)
	if err != nil {
		return "", err
	}

	if signature != expectedSignature {
		return "", NewAuthError("XAmzContentSHA256Mismatch", "The request signature we calculated does not match the signature you provided")
	}

	return accessKeyID, nil
}

// calculateSignatureV4Query calculates AWS Signature Version 4 for query string authentication
func (a *AWS4Authenticator) calculateSignatureV4Query(r *http.Request, secretAccessKey, date, region, service, signedHeaders string) (string, error) {
	// Step 1: Create canonical request (excluding signature from query string)
	canonicalRequest := a.createCanonicalRequestQuery(r, signedHeaders)

	// Step 2: Create string to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)

	hashedCanonicalRequest := sha256Hash(canonicalRequest)

	timestamp := r.URL.Query().Get("X-Amz-Date")

	stringToSign := strings.Join([]string{
		algorithm,
		timestamp,
		credentialScope,
		hashedCanonicalRequest,
	}, "\n")

	// Step 3: Calculate signature
	signingKey := CalculateSigningKey(secretAccessKey, date, region, service)

	signature := hmacSHA256(signingKey, []byte(stringToSign))

	return hex.EncodeToString(signature), nil
}

// calculateSignatureV4Header calculates AWS Signature Version 4
func (a *AWS4Authenticator) calculateSignatureV4Header(r *http.Request, secretAccessKey, date, region, service, signedHeaders string) (string, error) {
	// Step 1: Create canonical request
	canonicalRequest := a.createCanonicalRequestHeader(r, signedHeaders)

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
	signingKey := CalculateSigningKey(secretAccessKey, date, region, service)

	signature := hmacSHA256(signingKey, []byte(stringToSign))

	return hex.EncodeToString(signature), nil
}

// createCanonicalRequestQuery creates a canonical request for AWS Signature V4 query string authentication
func (a *AWS4Authenticator) createCanonicalRequestQuery(r *http.Request, signedHeaders string) string {
	return a.createCanonicalRequestInternal(r, signedHeaders, true)
}

// createCanonicalRequestHeader creates a canonical request for AWS Signature V4
func (a *AWS4Authenticator) createCanonicalRequestHeader(r *http.Request, signedHeaders string) string {
	return a.createCanonicalRequestInternal(r, signedHeaders, false)
}

func pathEscape(p string) string {
	item := strings.Split(p, "/")
	for i, v := range item {
		if v == "" {
			continue
		}
		item[i] = url.QueryEscape(v)
	}
	return strings.Join(item, "/")
}

// createCanonicalRequestInternal creates a canonical request for AWS Signature V4
// isQueryAuth: when true, excludes X-Amz-Signature from query params and uses UNSIGNED-PAYLOAD
func (a *AWS4Authenticator) createCanonicalRequestInternal(r *http.Request, signedHeaders string, isQueryAuth bool) string {
	// Method
	method := r.Method

	// URI
	uri := pathEscape(r.URL.Path)
	if uri == "" {
		uri = "/"
	}

	// Query string
	queryString := r.URL.Query()
	var queryParams []string
	for key := range queryString {
		// Skip the signature parameter for query string auth
		if isQueryAuth && key == "X-Amz-Signature" {
			continue
		}
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
	var payloadHash string
	if isQueryAuth {
		// For query string auth, it's typically UNSIGNED-PAYLOAD
		payloadHash = "UNSIGNED-PAYLOAD"
	} else {
		payloadHash = r.Header.Get("X-Amz-Content-Sha256")
		if payloadHash == "" {
			payloadHash = "UNSIGNED-PAYLOAD"
		}
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

// parseAmzDate parses AWS timestamp format (YYYYMMDDTHHMMSSZ)
func parseAmzDate(timestamp string) (time.Time, error) {
	return time.Parse("20060102T150405Z", timestamp)
}

// CalculateSigningKey derives the signing key from the secret access key
func CalculateSigningKey(secretAccessKey, date, region, service string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secretAccessKey), []byte(date))
	dateRegionKey := hmacSHA256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSHA256(dateRegionKey, []byte(service))
	signingKey := hmacSHA256(dateRegionServiceKey, []byte("aws4_request"))
	return signingKey
}
