package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PostPolicy represents the policy document for presigned POST
type PostPolicy struct {
	Expiration string        `json:"expiration"`
	Conditions []interface{} `json:"conditions"`
}

// PostPolicyCondition represents a single condition in the policy
type PostPolicyCondition struct {
	Type  string // "eq", "starts-with", "content-length-range"
	Field string
	Value interface{}
}

// ValidatePresignedPost validates a presigned POST request
func (a *AWS4Authenticator) ValidatePresignedPost(formValues map[string]string, fileSize int64, bucket string) error {
	// Extract required fields
	policyB64, ok := formValues["policy"]
	if !ok {
		return NewAuthError("InvalidArgument", "Policy field is required")
	}

	signature, ok := formValues["x-amz-signature"]
	if !ok {
		return NewAuthError("InvalidArgument", "x-amz-signature field is required")
	}

	credential, ok := formValues["x-amz-credential"]
	if !ok {
		return NewAuthError("InvalidArgument", "x-amz-credential field is required")
	}

	algorithm := formValues["x-amz-algorithm"]
	if algorithm != "" && algorithm != "AWS4-HMAC-SHA256" {
		return NewAuthError("InvalidArgument", "Invalid algorithm")
	}

	// Parse credential to get access key ID
	parts := strings.Split(credential, "/")
	if len(parts) < 5 {
		return NewAuthError("InvalidArgument", "Invalid credential format")
	}
	accessKeyID := parts[0]
	date := parts[1]
	region := parts[2]
	service := parts[3]

	// Get secret key
	secretKey, ok := a.credentials[accessKeyID]
	if !ok {
		return NewAuthError("InvalidAccessKeyId", "The AWS access key ID you provided does not exist in our records")
	}

	// Verify signature
	expectedSignature, err := calculatePostSignature(secretKey, date, region, service, policyB64)
	if err != nil {
		return NewAuthError("SignatureDoesNotMatch", fmt.Sprintf("Failed to calculate signature: %v", err))
	}

	if signature != expectedSignature {
		return NewAuthError("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided")
	}

	// Decode and validate policy
	policyBytes, err := base64.StdEncoding.DecodeString(policyB64)
	if err != nil {
		return NewAuthError("InvalidArgument", "Invalid policy encoding")
	}

	var policy PostPolicy
	if err := json.Unmarshal(policyBytes, &policy); err != nil {
		return NewAuthError("InvalidArgument", "Invalid policy JSON")
	}

	// Validate expiration
	expiration, err := time.Parse(time.RFC3339, policy.Expiration)
	if err != nil {
		return NewAuthError("InvalidArgument", "Invalid expiration format")
	}

	if time.Now().After(expiration) {
		return NewAuthError("AccessDenied", "Policy has expired")
	}

	// Add bucket to form values for validation
	if bucket != "" {
		formValues["bucket"] = bucket
	}

	// Validate conditions
	if err := validatePolicyConditions(policy.Conditions, formValues, fileSize); err != nil {
		return err
	}

	return nil
}

// calculatePostSignature calculates the signature for presigned POST
func calculatePostSignature(secretKey, date, region, service, policyB64 string) (string, error) {
	// Calculate signing key
	signingKey := CalculateSigningKey(secretKey, date, region, service)

	// Sign the policy
	h := hmac.New(sha256.New, signingKey)
	h.Write([]byte(policyB64))
	signature := hex.EncodeToString(h.Sum(nil))

	return signature, nil
}

// validatePolicyConditions validates all policy conditions against form values
func validatePolicyConditions(conditions []interface{}, formValues map[string]string, fileSize int64) error {
	for _, cond := range conditions {
		switch c := cond.(type) {
		case map[string]interface{}:
			// Exact match condition: {"bucket": "my-bucket"}
			for key, value := range c {
				if err := validateExactMatch(key, value, formValues); err != nil {
					return err
				}
			}
		case []interface{}:
			// Array conditions: ["starts-with", "$key", "prefix/"]
			if len(c) < 2 {
				return NewAuthError("InvalidArgument", "Invalid condition format")
			}

			operator, ok := c[0].(string)
			if !ok {
				return NewAuthError("InvalidArgument", "Invalid condition operator")
			}

			switch operator {
			case "eq":
				// Equality: ["eq", "$key", "value"]
				if len(c) != 3 {
					return NewAuthError("InvalidArgument", "Invalid eq condition format")
				}
				field, _ := c[1].(string)
				value := c[2]
				if err := validateExactMatch(strings.TrimPrefix(field, "$"), value, formValues); err != nil {
					return err
				}

			case "starts-with":
				// Prefix match: ["starts-with", "$key", "prefix"]
				if len(c) != 3 {
					return NewAuthError("InvalidArgument", "Invalid starts-with condition format")
				}
				field, _ := c[1].(string)
				prefix, _ := c[2].(string)
				fieldName := strings.TrimPrefix(field, "$")

				// Special handling for lowercase field names
				actualValue := formValues[fieldName]
				if actualValue == "" {
					// Try with different case variations
					for k, v := range formValues {
						if strings.EqualFold(k, fieldName) {
							actualValue = v
							break
						}
					}
				}

				// Empty prefix means any value is allowed
				if prefix != "" && !strings.HasPrefix(actualValue, prefix) {
					return NewAuthError("AccessDenied", fmt.Sprintf("Policy condition failed: %s does not start with %s", fieldName, prefix))
				}

			case "content-length-range":
				// Size range: ["content-length-range", min, max]
				if len(c) != 3 {
					return NewAuthError("InvalidArgument", "Invalid content-length-range condition format")
				}

				var minSize, maxSize int64
				switch v := c[1].(type) {
				case float64:
					minSize = int64(v)
				case int:
					minSize = int64(v)
				case string:
					parsed, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						return NewAuthError("InvalidArgument", "Invalid min size in content-length-range")
					}
					minSize = parsed
				default:
					return NewAuthError("InvalidArgument", "Invalid min size type in content-length-range")
				}

				switch v := c[2].(type) {
				case float64:
					maxSize = int64(v)
				case int:
					maxSize = int64(v)
				case string:
					parsed, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						return NewAuthError("InvalidArgument", "Invalid max size in content-length-range")
					}
					maxSize = parsed
				default:
					return NewAuthError("InvalidArgument", "Invalid max size type in content-length-range")
				}

				if fileSize < minSize || fileSize > maxSize {
					return NewAuthError("EntityTooLarge", fmt.Sprintf("File size %d is outside allowed range [%d, %d]", fileSize, minSize, maxSize))
				}

			default:
				// Unknown operators are ignored for compatibility
			}
		}
	}

	return nil
}

// validateExactMatch validates exact match condition
func validateExactMatch(field string, expectedValue interface{}, formValues map[string]string) error {
	// Get actual value from form
	actualValue := formValues[field]
	if actualValue == "" {
		// Try with different case variations
		for k, v := range formValues {
			if strings.EqualFold(k, field) {
				actualValue = v
				break
			}
		}
	}

	// Convert expected value to string for comparison
	var expectedStr string
	switch v := expectedValue.(type) {
	case string:
		expectedStr = v
	case float64:
		expectedStr = strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		expectedStr = strconv.Itoa(v)
	default:
		expectedStr = fmt.Sprintf("%v", v)
	}

	// Empty expected value means field can be anything (or omitted)
	if expectedStr != "" && actualValue != expectedStr {
		return NewAuthError("AccessDenied", fmt.Sprintf("Policy condition failed: %s value mismatch", field))
	}

	return nil
}
