package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type DB struct {
	Host     string
	Port     int
	Name     string
	User     string
	Password string
	SSLMode  string
}

func (d DB) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.Host, d.Port, d.Name, d.User, d.Password, d.SSLMode,
	)
}

type S3 struct {
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Prefix    string
}

type Config struct {
	DB        DB
	S3        S3
	Table     string
	Gzip      bool
	Timezone  *time.Location
	Retries   int
	RetryWait time.Duration
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("env %s: %w", key, err)
	}
	return v, nil
}

func getenvBool(key string, def bool) (bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("env %s: %w", key, err)
	}
	return v, nil
}

func Load() (Config, error) {
	port, err := getenvInt("DB_PORT", 5432)
	if err != nil {
		return Config{}, err
	}
	gzip, err := getenvBool("EXPORT_GZIP", false)
	if err != nil {
		return Config{}, err
	}
	useSSL, err := getenvBool("S3_USE_SSL", false)
	if err != nil {
		return Config{}, err
	}
	retries, err := getenvInt("RETRY_COUNT", 30)
	if err != nil {
		return Config{}, err
	}
	retryWaitSec, err := getenvInt("RETRY_DELAY_SECONDS", 5)
	if err != nil {
		return Config{}, err
	}

	tzName := getenv("TZ", "UTC")
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return Config{}, fmt.Errorf("env TZ=%q: %w", tzName, err)
	}

	cfg := Config{
		Table:     getenv("TABLE_NAME", "shipments"),
		Gzip:      gzip,
		Timezone:  loc,
		Retries:   retries,
		RetryWait: time.Duration(retryWaitSec) * time.Second,
		DB: DB{
			Host:     getenv("DB_HOST", "localhost"),
			Port:     port,
			Name:     getenv("DB_NAME", "shipping"),
			User:     getenv("DB_USER", "exporter"),
			Password: getenv("DB_PASSWORD", ""),
			SSLMode:  getenv("DB_SSLMODE", "disable"),
		},
		S3: S3{
			Endpoint:  getenv("S3_ENDPOINT", "localhost:9000"),
			Region:    getenv("S3_REGION", "us-east-1"),
			Bucket:    getenv("S3_BUCKET", "analytics"),
			AccessKey: getenv("S3_ACCESS_KEY", ""),
			SecretKey: getenv("S3_SECRET_KEY", ""),
			UseSSL:    useSSL,
			Prefix:    getenv("S3_PREFIX", "shipments"),
		},
	}

	if cfg.DB.Password == "" {
		return Config{}, fmt.Errorf("env DB_PASSWORD is required")
	}
	return cfg, nil
}
