// Package storage keeps the long-lived files of Nabu in S3 (MinIO): files of
// personal spaces, attachments, session snapshots and skills snapshots
// (FTR.NAB.CMN-0001 arch §10).
package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrNotFound is a missing object.
var ErrNotFound = errors.New("object not found")

// Object describes a stored object.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Storage is an object store.
type Storage interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]Object, error)
}

// S3 is a MinIO-client backed store.
type S3 struct {
	c      *minio.Client
	bucket string
}

// NewS3 connects to the endpoint and ensures the bucket exists (the first
// installation creates the bucket nabu this way, tech spec §11).
func NewS3(ctx context.Context, endpoint, accessKey, secretKey, bucket string, useSSL bool) (*S3, error) {
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: useSSL})
	if err != nil {
		return nil, err
	}
	s := &S3{c: c, bucket: bucket}
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Put stores an object; size -1 streams with an unknown length.
func (s *S3) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.c.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: contentType})
	return err
}

// Get opens an object; a missing object is ErrNotFound.
func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	o, err := s.c.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	if _, err := o.Stat(); err != nil {
		o.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return o, nil
}

// Delete removes an object.
func (s *S3) Delete(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// List lists the objects under prefix.
func (s *S3) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, Object{Key: o.Key, Size: o.Size, LastModified: o.LastModified})
	}
	return out, nil
}

// ReadAll reads a whole object (small objects only: snapshots, bundles).
func ReadAll(ctx context.Context, s Storage, key string) ([]byte, error) {
	rc, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
