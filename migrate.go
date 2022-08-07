package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type MigrationService struct {
	DB       *DataBase
	Uploader *manager.Uploader
}

func NewMigrationService(db *DataBase, up *manager.Uploader) MigrationService {
	return MigrationService{
		DB:       db,
		Uploader: up,
	}
}

func (srv MigrationService) Migrate(cxt context.Context, beginDate, endDate string, bucket string) error {
	return nil
}

func (srv MigrationService) UploadFiles(ctx context.Context, bucket string, files []string) error {

	for num, path := range files {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open %q:%w", path, err)
		}

		key := path

		_, err = srv.Uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload %q:%w", path, err)
		}
		log.Printf("%d: %q uploaded", num, path)
	}

	return nil
}
