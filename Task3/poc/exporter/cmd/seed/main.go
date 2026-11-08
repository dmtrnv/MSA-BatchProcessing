package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"shipment-export/internal/config"
	"shipment-export/internal/seed"
	"strconv"
	"syscall"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	opts := seed.Options{
		DB:               cfg.DB,
		Clients:          envInt("SEED_CLIENTS", 10000),
		Drivers:          envInt("SEED_DRIVERS", 1000),
		Vehicles:         envInt("SEED_VEHICLES", 500),
		Shipments:        envInt("SEED_SHIPMENTS", 50000),
		Events:           envInt("SEED_EVENTS", 500000),
		Seed:             int64(envInt("SEED_RANDOM", 42)),
		ExporterUser:     envStr("EXPORTER_DB_USER", "exporter"),
		ExporterPassword: envStr("EXPORTER_DB_PASSWORD", ""),
		Retries:          cfg.Retries,
		RetryWait:        cfg.RetryWait,
	}
	if opts.ExporterPassword == "" {
		logger.Error("env EXPORTER_DB_PASSWORD is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := seed.Run(ctx, opts, logger); err != nil {
		logger.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		panic(fmt.Sprintf("env %s=%q: %v", key, raw, err))
	}
	return v
}
