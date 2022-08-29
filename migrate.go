package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type File struct {
	ID       int
	FilePath string
	DirName  string
	Migrated bool
	Stored   bool
}

type MigrationService struct {
	DB       *DataBase
	Uploader *Uploader

	numMigratedFiles uint64
}

func NewMigrationService(db *DataBase, up *Uploader) *MigrationService {
	return &MigrationService{
		DB:       db,
		Uploader: up,
	}
}

func (srv *MigrationService) Migrate(ctx context.Context, beginDate, endDate string) error {
	for i := 1; ; i++ {
		start := time.Now()

		log.Println("fetching files")
		files, err := srv.DB.GetFilesToMigrate(ctx, beginDate, endDate)
		if err != nil {
			return fmt.Errorf("failed to fetch files:%w", err)
		}
		log.Printf("fetched %d files", len(files))
		if len(files) == 0 {
			break
		}

		log.Println("uploading files")
		if err := srv.UploadFiles(ctx, files); err != nil {
			return fmt.Errorf("failed to upload files:%w", err)
		}

		log.Println("cleaning up table with prev files")
		if err := srv.DB.CleanupMigratedFiles(ctx); err != nil {
			return fmt.Errorf("failed to cleanup prev migrated files:%w", err)
		}
		log.Println("adding files into table")
		if err := srv.DB.AddMigratedFiles(ctx, files); err != nil {
			return fmt.Errorf("failed to add migrated files:%w", err)
		}
		log.Println("marking files as migrated")
		if err := srv.DB.MarkMigratedFiles(ctx); err != nil {
			return fmt.Errorf("failed to mark migrated files:%w", err)
		}

		// TODO
		// log.Println("removing files")
		// if err := srv.removeFiles(ctx, files); err != nil {
		// 	return fmt.Errorf("failed to remove files:%w", err)
		// }

		elapsed := time.Since(start)
		// TODO migrated - fix
		log.Printf("%d migrated| %s elapsed| total %d", len(files), elapsed, atomic.LoadUint64(&srv.numMigratedFiles))
	}
	return nil
}

func (srv *MigrationService) UploadFiles(ctx context.Context, files []File) error {
	ch := make(chan *File)

	maxGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(maxGoroutines)

	for i := 1; i <= maxGoroutines; i++ {
		go func() {
			defer wg.Done()

			for file := range ch {
				key := srv.getKey(*file)
				err := srv.Uploader.UploadFile(ctx, file.FilePath, key)
				if err == nil {
					file.DirName = srv.getNewDir(*file)
					file.Migrated = true
					file.Stored = true

					atomic.AddUint64(&srv.numMigratedFiles, 1)
				} else if err != ErrFileNotExist {
					log.Printf("Error Upload File %s:%v", file.FilePath, err)
				}
			}
		}()
	}

	for i := range files {
		select {
		case <-ctx.Done():
			close(ch)
			return ctx.Err()
		default:
		}

		ch <- &files[i]
		// time.Sleep(1 * time.Second)
	}
	close(ch)
	wg.Wait()

	/*
		for i := range files {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			key := srv.getKey(files[i])
			if err := srv.Uploader.UploadFile(ctx, files[i].FilePath, key); err != nil {
				if os.IsNotExist(err) {
					log.Printf("File Does Not Exist:%s", files[i].FilePath)
					continue
				}
				log.Printf("Error Upload File %s:%v", files[i].FilePath, err)
				continue
				//return fmt.Errorf("failed to upload %q:%v", key, err)
			}
			files[i].DirName = srv.getNewDir(files[i])
			files[i].Migrated = true

			srv.numMigratedFiles++
			// log.Printf("%d\t%d\t%s", srv.numMigratedFiles, files[i].ID, files[i].FilePath)
		}
	*/

	return nil
}

func (srv *MigrationService) getKey(file File) string {
	ss := strings.Split(file.FilePath, "\\")
	name := ss[len(ss)-1]
	dir := ss[len(ss)-2]
	return fmt.Sprintf("%s/%s", dir, name)
}

func (srv *MigrationService) getNewDir(file File) string {
	ss := strings.Split(file.FilePath, "\\")
	dir := ss[len(ss)-2]
	return fmt.Sprintf("%s/%s", srv.Uploader.Bucket, dir)
}

func (cl *MigrationService) removeFiles(ctx context.Context, files []File) error {
	for _, file := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !file.Migrated {
			continue
		}

		if err := os.Remove(file.FilePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("failed to remove file(%d %s):%w", file.ID, file.FilePath, err)
		}
	}
	return nil
}
