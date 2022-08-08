package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

/*
	https://storage.yandexcloud.net/demo-4kam-images/testdata/panorama_image_part_5_da68cfb2-6f50-4568-a3b9-09d49d7e29df.jpg
*/
func main() {
	ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	cfg, err := setup()
	if err != nil {
		log.Fatalf("failed to setup:%v", err)
	}

	db := NewDataBase(cfg.DSN)
	uploader, err := NewUploader(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.Bucket)
	if err != nil {
		log.Fatalf("failed to build S3 client:%v", err)
	}

	srv := NewMigrationService(db, uploader)

	/* TEST

	err = srv.UploadFiles(ctx, []string{
		"../../testdata/download_photo_0ee29e4c-1c50-4044-aefa-13634093a7b51658494326425.jpg",
		"../../testdata/hotfield_55b24e10-13bf-4d96-979f-e709fd5fdb111659230027453.jpg",
		"../../testdata/hotfield_c6a20d03-c3ea-4c7a-a0fa-fb69084e2dc51658457974482.jpg",
		"../../testdata/panorama_image_part_5_da68cfb2-6f50-4568-a3b9-09d49d7e29df.jpg",
	})
	*/

	err = srv.Migrate(ctx, cfg.BeginDate, cfg.EndDate)
	done()

	if err != nil {
		log.Fatalf("failed to migrate for %s to %s:%v", cfg.BeginDate, cfg.EndDate, err)
	}
	log.Printf("migrating %d files for %s to %s is successful", srv.numMigratedFiles, cfg.BeginDate, cfg.EndDate)
}

type сonfig struct {
	DSN                string
	BeginDate, EndDate string

	AccessKeyID     string
	SecretAccessKey string

	Bucket string
}

func setup() (сonfig, error) {
	var cfg сonfig
	var ok bool

	if cfg.DSN, ok = os.LookupEnv("DATABASE_URL"); !ok {
		return cfg, fmt.Errorf("DATABASE_URL must be set")
	}
	if cfg.BeginDate, ok = os.LookupEnv("BEGIN_DATE"); !ok {
		return cfg, fmt.Errorf("BEGIN_DATE must be set")
	}
	if cfg.EndDate, ok = os.LookupEnv("END_DATE"); !ok {
		return cfg, fmt.Errorf("END_DATE must be set")
	}
	if cfg.AccessKeyID, ok = os.LookupEnv("AWS_ACCESS_KEY_ID"); !ok {
		return cfg, fmt.Errorf("AWS_ACCESS_KEY_ID must be set")
	}
	if cfg.SecretAccessKey, ok = os.LookupEnv("AWS_SECRET_ACCESS_KEY"); !ok {
		return cfg, fmt.Errorf("AWS_SECRET_ACCESS_KEY must be set")
	}
	if cfg.Bucket, ok = os.LookupEnv("BUCKET_NAME"); !ok {
		return cfg, fmt.Errorf("BUCKET_NAME must be set")
	}

	return cfg, nil
}
