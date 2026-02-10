package storage

import (
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func isBucketNameValid(bucket string) bool {
	if bucket == "" {
		return false
	}

	if bucket[0] == '-' || bucket[len(bucket)-1] == '-' {
		return false
	}

	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}

	for _, c := range bucket {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-':
		case c == '.':
		default:
			return false
		}
	}

	return len(bucket) <= 63
}

// CreateBucket creates a new bucket
func (s *Storage) CreateBucket(bucket string) error {
	if !isBucketNameValid(bucket) {
		return ErrInvalidBucketName
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b != nil {
			return ErrBucketAlreadyExists
		}

		_, err := tx.CreateBucket([]byte(contentBucketPrefix + bucket))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte(uploadsBucketPrefix + bucket))
		if err != nil {
			return err
		}
		b, err = tx.CreateBucket([]byte(metadataBucketPrefix + bucket))
		if err != nil {
			return err
		}

		err = b.Put([]byte("modtime"), []byte(time.Now().UTC().Format(time.RFC3339Nano)))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteBucket deletes a bucket
func (s *Storage) DeleteBucket(bucket string) error {
	if !isBucketNameValid(bucket) {
		return ErrInvalidBucketName
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		// TODO: Clean up all objects and multipart uploads in the bucket before deleting

		err := tx.DeleteBucket([]byte(contentBucketPrefix + bucket))
		if err != nil {
			return err
		}
		err = tx.DeleteBucket([]byte(uploadsBucketPrefix + bucket))
		if err != nil {
			return err
		}
		err = tx.DeleteBucket([]byte(metadataBucketPrefix + bucket))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// ListBuckets lists all buckets with pagination support
func (s *Storage) ListBuckets(prefix, continuationToken string, maxBuckets int) (buckets []BucketInfo, nextContinuationToken string, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()

		var count int
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			name := string(k)
			if !strings.HasPrefix(name, contentBucketPrefix) {
				continue
			}
			bucketName := strings.TrimPrefix(name, contentBucketPrefix)
			if prefix != "" && !strings.HasPrefix(bucketName, prefix) {
				continue
			}
			if continuationToken != "" && bucketName <= continuationToken {
				continue
			}

			modtimeBytes := tx.Bucket([]byte(metadataBucketPrefix + bucketName)).Get([]byte("modtime"))
			modtime, _ := time.Parse(time.RFC3339Nano, string(modtimeBytes))

			buckets = append(buckets, BucketInfo{
				Name:    bucketName,
				ModTime: modtime,
			})
			count++
			if count >= maxBuckets {
				nextContinuationToken = bucketName
				break
			}
		}
		return nil
	})
	return buckets, nextContinuationToken, err
}

// BucketExists checks if a bucket exists
func (s *Storage) BucketExists(bucket string) bool {
	if !isBucketNameValid(bucket) {
		return false
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(contentBucketPrefix + bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		return nil
	})
	return err == nil
}
