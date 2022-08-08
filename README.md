# Migrate 4kam photos to S3(Yandex Object Storage)
## Build
```bash
make
```

## Run on Windows
```bash
SET BEGIN_DATE=20191001
SET END_DATE=20191002
SET DATABASE_URL=
SET AWS_ACCESS_KEY_ID=
SET AWS_SECRET_ACCESS_KEY=
SET AWS_REGION=ru-central1
SET BUCKET_NAME=

migrate.exe
```