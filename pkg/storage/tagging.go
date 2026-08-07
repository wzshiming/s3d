package storage

// GetObjectTagging returns the tag set of an object
func (s *Storage) GetObjectTagging(bucket, key string) ([]Tag, error) {
	if !s.BucketExists(bucket) {
		return nil, ErrBucketNotFound
	}

	metadata, err := s.getObjectMetadata(bucket, key)
	if err != nil {
		return nil, err
	}

	return metadata.Metadata.Tagging, nil
}

// PutObjectTagging replaces the tag set of an object
func (s *Storage) PutObjectTagging(bucket, key string, tags []Tag) error {
	if !s.BucketExists(bucket) {
		return ErrBucketNotFound
	}

	metadata, err := s.getObjectMetadata(bucket, key)
	if err != nil {
		return err
	}

	metadata.Metadata.Tagging = tags
	return s.putObjectMetadata(bucket, key, metadata)
}

// DeleteObjectTagging removes all tags from an object
func (s *Storage) DeleteObjectTagging(bucket, key string) error {
	return s.PutObjectTagging(bucket, key, nil)
}
