package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MigrationService_getKey(t *testing.T) {
	file := File{
		FilePath: `C:\inetpub\4kam_files\disk2\20200102\panorama_image_part9199eb62c-3464-4e25-ac31-37b325e9f26d.jpg`,
	}

	srv := MigrationService{}
	key := srv.getKey(file)

	want := `20200102/panorama_image_part9199eb62c-3464-4e25-ac31-37b325e9f26d.jpg`
	assert.Equal(t, want, key)
}

func Test_MigrationService_getNewDir(t *testing.T) {
	file := File{
		FilePath: `C:\inetpub\4kam_files\disk2\20200102\panorama_image_part9199eb62c-3464-4e25-ac31-37b325e9f26d.jpg`,
	}

	srv := MigrationService{
		Uploader: &Uploader{
			Bucket: "bucket",
		},
	}
	dirName := srv.getNewDir(file)

	want := `bucket/20200102`
	assert.Equal(t, want, dirName)
}
