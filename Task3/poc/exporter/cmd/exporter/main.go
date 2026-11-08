package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"shipment-export/internal/config"
	"shipment-export/internal/export"
	"syscall"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := export.Run(ctx, cfg, logger); err != nil {
		logger.Error("export failed", "error", err)
		os.Exit(1)
	}
}
