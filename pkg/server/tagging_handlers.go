package server

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"unicode/utf8"

	"github.com/wzshiming/s3d/pkg/storage"
)

const (
	// maxObjectTags is the maximum number of tags per object
	maxObjectTags = 10
	// maxTagKeyLength is the maximum length of a tag key
	maxTagKeyLength = 128
	// maxTagValueLength is the maximum length of a tag value
	maxTagValueLength = 256
)

// validateTags validates a tag set against the S3 object tagging limits
func validateTags(tags []storage.Tag) error {
	if len(tags) > maxObjectTags {
		return errors.New("Object tags cannot be greater than 10")
	}
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if tag.Key == "" || utf8.RuneCountInString(tag.Key) > maxTagKeyLength {
			return errors.New("The TagKey you have provided is invalid")
		}
		if utf8.RuneCountInString(tag.Value) > maxTagValueLength {
			return errors.New("The TagValue you have provided is invalid")
		}
		if _, exists := seen[tag.Key]; exists {
			return errors.New("Cannot provide multiple Tags with the same key")
		}
		seen[tag.Key] = struct{}{}
	}
	return nil
}

// sortTags sorts a tag set by key for a deterministic order
func sortTags(tags []storage.Tag) {
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })
}

// parseTaggingHeader parses the URL-encoded x-amz-tagging header into a tag set sorted by key
func parseTaggingHeader(header string) ([]storage.Tag, error) {
	values, err := url.ParseQuery(header)
	if err != nil {
		return nil, err
	}
	tags := make([]storage.Tag, 0, len(values))
	for key, vals := range values {
		for _, val := range vals {
			tags = append(tags, storage.Tag{Key: key, Value: val})
		}
	}
	sortTags(tags)
	return tags, nil
}

// tagsFromXML converts a Tagging XML document into a storage tag set
func tagsFromXML(tagging Tagging) []storage.Tag {
	tags := make([]storage.Tag, 0, len(tagging.TagSet.Tags))
	for _, tag := range tagging.TagSet.Tags {
		tags = append(tags, storage.Tag{Key: tag.Key, Value: tag.Value})
	}
	return tags
}

// extractTagging parses and validates the x-amz-tagging header.
// It writes an error response and returns false if the header is invalid.
func (s *S3Handler) extractTagging(w http.ResponseWriter, r *http.Request) ([]storage.Tag, bool) {
	header := r.Header.Get("x-amz-tagging")
	if header == "" {
		return nil, true
	}
	tags, err := parseTaggingHeader(header)
	if err != nil {
		s.response(w, r, "InvalidArgument", "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.", http.StatusBadRequest)
		return nil, false
	}
	if err := validateTags(tags); err != nil {
		s.response(w, r, "InvalidTag", err.Error(), http.StatusBadRequest)
		return nil, false
	}
	return tags, true
}

// handleGetObjectTagging handles GetObjectTagging operation
func (s *S3Handler) handleGetObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	tags, err := s.storage.GetObjectTagging(bucket, key)
	if err != nil {
		s.errorResponse(w, r, err)
		return
	}

	result := Tagging{}
	for _, tag := range tags {
		result.TagSet.Tags = append(result.TagSet.Tags, Tag{Key: tag.Key, Value: tag.Value})
	}

	s.xmlResponse(w, r, result, http.StatusOK)
}

// handlePutObjectTagging handles PutObjectTagging operation
func (s *S3Handler) handlePutObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var tagging Tagging
	if err := xml.NewDecoder(r.Body).Decode(&tagging); err != nil {
		s.response(w, r, "MalformedXML", "The XML you provided was not well-formed", http.StatusBadRequest)
		return
	}

	tags := tagsFromXML(tagging)
	if err := validateTags(tags); err != nil {
		s.response(w, r, "InvalidTag", err.Error(), http.StatusBadRequest)
		return
	}
	sortTags(tags)

	if err := s.storage.PutObjectTagging(bucket, key, tags); err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteObjectTagging handles DeleteObjectTagging operation
func (s *S3Handler) handleDeleteObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := s.storage.DeleteObjectTagging(bucket, key); err != nil {
		s.errorResponse(w, r, err)
		return
	}

	s.setHeaders(w, r)
	w.WriteHeader(http.StatusNoContent)
}
