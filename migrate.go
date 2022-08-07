package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
)

type MigrationService struct {
	DB       *DataBase
	Uploader *Uploader
}

func NewMigrationService(db *DataBase, up *Uploader) MigrationService {
	return MigrationService{
		DB:       db,
		Uploader: up,
	}
}

func (srv MigrationService) Migrate(cxt context.Context, beginDate, endDate string, bucket string) error {
	return nil
}

func (srv MigrationService) UploadFiles(ctx context.Context, files []string) error {
	for num, path := range files {
		key := filepath.Join("testdata", filepath.Base(path))
		if err := srv.Uploader.UploadFile(ctx, path, key); err != nil {
			return fmt.Errorf("failed to upload %q:%v", key, err)
		}
		log.Printf("%d: %q uploaded", num, path)
	}

	return nil
}
