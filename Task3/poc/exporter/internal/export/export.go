package export

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"shipment-export/internal/config"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

var tableNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) (err error) {
	started := time.Now()

	if !tableNameRe.MatchString(cfg.Table) {
		return fmt.Errorf("invalid table name %q", cfg.Table)
	}
	if cfg.S3.AccessKey == "" || cfg.S3.SecretKey == "" {
		return fmt.Errorf("env S3_ACCESS_KEY and S3_SECRET_KEY are required")
	}

	conn, err := connect(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	tmp, err := os.CreateTemp("", cfg.Table+"-*.csv")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if cerr := os.Remove(tmpPath); cerr != nil && !os.IsNotExist(cerr) {
			logger.Warn("could not remove temp file", "path", tmpPath, "error", cerr)
		}
	}()

	rows, err := writeCSV(ctx, conn, cfg, tmp)
	tmp.Close()
	if err != nil {
		return err
	}

	size, err := fileSize(tmpPath)
	if err != nil {
		return err
	}

	objectKey := buildObjectKey(cfg)
	if err := upload(ctx, cfg, logger, tmpPath, objectKey); err != nil {
		return err
	}

	logger.Info("export finished",
		"table", cfg.Table,
		"rows", rows,
		"bytes", size,
		"object", objectKey,
		"bucket", cfg.S3.Bucket,
		"duration", time.Since(started).String(),
	)
	return nil
}

func connect(ctx context.Context, cfg config.Config, logger *slog.Logger) (*pgx.Conn, error) {
	var lastErr error
	for attempt := 1; attempt <= cfg.Retries; attempt++ {
		conn, err := pgx.Connect(ctx, cfg.DB.DSN())
		if err == nil {
			return conn, nil
		}
		lastErr = err
		logger.Warn("database not ready, retrying",
			"attempt", attempt, "of", cfg.Retries, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(cfg.RetryWait):
		}
	}
	return nil, fmt.Errorf("connect to database after %d attempts: %w", cfg.Retries, lastErr)
}

func writeCSV(ctx context.Context, conn *pgx.Conn, cfg config.Config, dst io.Writer) (int64, error) {
	var out io.Writer = dst
	var gz *gzip.Writer
	if cfg.Gzip {
		gz = gzip.NewWriter(dst)
		out = gz
	}

	sql := fmt.Sprintf(
		"COPY (SELECT * FROM %s) TO STDOUT WITH (FORMAT csv, HEADER true)",
		pgx.Identifier{cfg.Table}.Sanitize(),
	)
	tag, err := conn.PgConn().CopyTo(ctx, out, sql)
	if err != nil {
		return 0, fmt.Errorf("copy %s: %w", cfg.Table, err)
	}

	if gz != nil {
		if err := gz.Close(); err != nil {
			return 0, fmt.Errorf("close gzip: %w", err)
		}
	}
	return tag.RowsAffected(), nil
}

func buildObjectKey(cfg config.Config) string {
	date := time.Now().In(cfg.Timezone).Format("2006-01-02")
	prefix := cfg.S3.Prefix
	if prefix == "" {
		prefix = cfg.Table
	}
	name := fmt.Sprintf("%s_%s.csv", cfg.Table, date)
	if cfg.Gzip {
		name += ".gz"
	}
	return filepath.ToSlash(filepath.Join(prefix, date, name))
}

func upload(ctx context.Context, cfg config.Config, logger *slog.Logger, path, key string) error {
	client, err := minio.New(cfg.S3.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3.AccessKey, cfg.S3.SecretKey, ""),
		Secure: cfg.S3.UseSSL,
		Region: cfg.S3.Region,
	})
	if err != nil {
		return fmt.Errorf("create minio client: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.S3.Bucket)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", cfg.S3.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.S3.Bucket, minio.MakeBucketOptions{Region: cfg.S3.Region}); err != nil {
			return fmt.Errorf("create bucket %q: %w", cfg.S3.Bucket, err)
		}
		logger.Info("created bucket", "bucket", cfg.S3.Bucket)
	}

	contentType := "text/csv"
	if cfg.Gzip {
		contentType = "application/gzip"
	}

	info, err := client.FPutObject(ctx, cfg.S3.Bucket, key, path, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return fmt.Errorf("upload object %q: %w", key, err)
	}
	logger.Info("uploaded object", "bucket", cfg.S3.Bucket, "object", key, "size", info.Size)
	return nil
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat temp file: %w", err)
	}
	return info.Size(), nil
}
