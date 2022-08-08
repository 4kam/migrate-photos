package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	driver "github.com/denisenkom/go-mssqldb"
)

type DataBase struct {
	*sql.DB
}

func NewDataBase(dsn string) *DataBase {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Panicf("failed to open conn to database: %v", err)
	}

	return &DataBase{db}
}

func (db *DataBase) inTx(ctx context.Context, isoLevel sql.IsolationLevel, f func(tx *sql.Tx) error) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping to database: %v", err)
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: isoLevel})
	if err != nil {
		return fmt.Errorf("starting transaction: %v", err)
	}

	if err := f(tx); err != nil {
		if err1 := tx.Rollback(); err1 != nil {
			return fmt.Errorf("rolling back transaction: %v (original error: %v)", err1, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %v", err)
	}
	return nil
}

func (db *DataBase) GetFilesToMigrate(ctx context.Context, beginDate, endDate string) ([]File, error) {
	var files []File

	if err := db.inTx(ctx, sql.LevelReadCommitted, func(tx *sql.Tx) error {
		rows, err := db.QueryContext(ctx,
			`[dbo].[GetFilesToMigrate]`,
			sql.Named("beginDate", beginDate),
			sql.Named("endDate", endDate),
		)
		if err != nil {
			return fmt.Errorf("failed to list: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Err(); err != nil {
				return fmt.Errorf("failed to iterate: %w", err)
			}

			var file File
			if err = rows.Scan(&file.ID, &file.FilePath); err != nil {
				return fmt.Errorf("failed to parse: %w", err)
			}
			files = append(files, file)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}

	return files, nil
}

func (db *DataBase) CleanupMigratedFiles(ctx context.Context) error {
	return db.inTx(ctx, sql.LevelReadCommitted, func(tx *sql.Tx) error {
		result, err := db.ExecContext(ctx, "truncate table dbo.MigratedFiles;")

		if err != nil {
			return fmt.Errorf("failed to cleanup MigratedFiles: %w", err)
		}
		if _, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("failed to RowsAffected: %w", err)
		}

		return nil
	})
}

func (db *DataBase) AddMigratedFiles(ctx context.Context, files []File) error {
	return db.inTx(ctx, sql.LevelReadCommitted, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, driver.CopyIn("[dbo].[MigratedFiles]", driver.BulkOptions{},
			"FileID", "DirName"))
		if err != nil {
			return fmt.Errorf("failed to prepare: %v", err)
		}
		defer stmt.Close()

		for _, v := range files {
			if !v.Migrated {
				continue
			}
			if _, err = stmt.ExecContext(ctx, v.ID, v.DirName); err != nil {
				return fmt.Errorf("failed to add migrated files: %w", err)
			}
		}

		result, err := stmt.ExecContext(ctx)
		if err != nil {
			return fmt.Errorf("failed to add migrated files: %w", err)
		}
		if _, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("failed to RowsAffected: %w", err)
		}

		return nil
	})
}

func (db *DataBase) MarkMigratedFiles(ctx context.Context) error {
	return db.inTx(ctx, sql.LevelReadCommitted, func(tx *sql.Tx) error {
		result, err := db.ExecContext(ctx, "[dbo].[MarkMigratedFiles]")
		if err != nil {
			return fmt.Errorf("failed to mark migrated files: %w", err)
		}
		if _, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("failed RowsAffected: %w", err)
		}
		return nil
	})
}
