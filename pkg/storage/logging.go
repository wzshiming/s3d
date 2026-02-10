package storage

import (
	"bytes"
	"encoding/gob"

	bolt "go.etcd.io/bbolt"
)

// GetBucketLogging retrieves the logging configuration for a bucket
func (s *Storage) GetBucketLogging(bucket string) (bucketLogging *BucketLogging, err error) {
	if !isBucketNameValid(bucket) {
		return nil, ErrInvalidBucketName
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucketPrefix + bucket))
		if b == nil {
			return ErrBucketLoggingNotFound
		}
		val := b.Get([]byte("logging"))
		if len(val) == 0 {
			return ErrBucketLoggingNotFound
		}

		decoder := gob.NewDecoder(bytes.NewReader(val))
		if err := decoder.Decode(&bucketLogging); err != nil {
			return err
		}
		return nil

	})
	if err != nil {
		return nil, err
	}
	return bucketLogging, nil
}

// PutBucketLogging sets the logging configuration for a bucket
func (s *Storage) PutBucketLogging(bucket string, bucketLogging *BucketLogging) error {
	if !isBucketNameValid(bucket) {
		return ErrInvalidBucketName
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(bucketLogging); err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}

		return b.Put([]byte("logging"), buf.Bytes())
	})
}

// DeleteBucketLogging deletes the logging configuration for a bucket
func (s *Storage) DeleteBucketLogging(bucket string) error {
	if !isBucketNameValid(bucket) {
		return ErrInvalidBucketName
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		return b.Delete([]byte("logging"))
	})
}
