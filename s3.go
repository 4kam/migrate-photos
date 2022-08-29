package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	ErrFileNotExist = errors.New("file does not exist")
)

type Uploader struct {
	cl *s3.Client

	Bucket string
}

func (upl *Uploader) Upload(ctx context.Context, r io.Reader, key string) error {
	_, err := upl.cl.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(upl.Bucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String("image/jpg"),
	})
	if err != nil {
		return fmt.Errorf("failed to PutObject %s:%w", key, err)
	}
	return nil
}

func (upl *Uploader) UploadFile(ctx context.Context, path string, key string) error {
	file, err := os.Open(path)
	if err != nil {
		return ErrFileNotExist //fmt.Errorf("failed to open file(%q):%w", path, err)
	}
	defer file.Close()

	if err := upl.Upload(ctx, file, key); err != nil {
		return fmt.Errorf("failed to Upload(%q):%w", path, err)
	}

	return nil
}

func (upl *Uploader) CleanupBucket(ctx context.Context, bucket string) error {
	result, err := upl.cl.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to ListObjectsV2:%w", err)
	}

	for _, object := range result.Contents {
		upl.cl.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    object.Key,
		})

		log.Printf("object=%s deleted", aws.ToString(object.Key))
	}

	return nil
}

func NewUploader(key, secret string, bucket string) (*Uploader, error) {
	cl, err := NewClient(key, secret)
	if err != nil {
		return nil, fmt.Errorf("failed to NewClient:%w", err)
	}
	up := &Uploader{
		cl:     cl,
		Bucket: bucket,
	}

	return up, nil
}

func NewClient(key, secret string) (*s3.Client, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID && region == "ru-central1" {
			return aws.Endpoint{
				PartitionID:   "yc",
				URL:           "https://storage.yandexcloud.net",
				SigningRegion: "ru-central1",
			}, nil
		}
		return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		/*
			from env
		*/
		// config.WithRegion("ru-central1"),
		// config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
		config.WithEndpointResolverWithOptions(customResolver))

	if err != nil {
		return nil, fmt.Errorf("failed to LoadDefaultConfig:%w", err)
	}

	return s3.NewFromConfig(cfg), nil
}
