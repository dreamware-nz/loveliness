package backup

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Store stores backups in an S3 bucket with an optional key prefix.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// S3Config holds configuration for the S3 backup store.
type S3Config struct {
	Bucket   string
	Region   string
	Prefix   string // optional key prefix, e.g. "backups/cluster-1/"
	Endpoint string // optional custom endpoint for S3-compatible stores (MinIO, R2, etc.)
}

// NewS3Store creates an S3 backup store. AWS credentials are resolved from the
// standard chain (env vars, shared credentials file, IAM role, etc.).
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // required for MinIO/R2
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	return &S3Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

func (s *S3Store) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + key
}

func (s *S3Store) Put(key string, r io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.fullKey(key)),
		Body:        r,
		ContentType: aws.String("application/gzip"),
	})
	return err
}

func (s *S3Store) Get(key string) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (s *S3Store) List() ([]BackupMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	}
	if s.prefix != "" {
		input.Prefix = aws.String(s.prefix)
	}

	var metas []BackupMeta
	paginator := s3.NewListObjectsV2Paginator(s.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if s.prefix != "" && len(key) > len(s.prefix) {
				key = key[len(s.prefix):]
			}
			metas = append(metas, BackupMeta{
				Key:       key,
				SizeBytes: aws.ToInt64(obj.Size),
				CreatedAt: aws.ToTime(obj.LastModified),
			})
		}
	}

	sort.Slice(metas, func(i, j int) bool {
		return metas[i].CreatedAt.After(metas[j].CreatedAt)
	})
	return metas, nil
}

func (s *S3Store) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	return err
}
