package auth

import (
	"encoding/xml"
)

// Error represents an S3 error response
type Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}
