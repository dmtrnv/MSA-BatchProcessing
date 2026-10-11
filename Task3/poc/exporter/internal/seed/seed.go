package seed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"shipment-export/internal/config"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type Options struct {
	DB               config.DB
	Clients          int
	Drivers          int
	Vehicles         int
	Shipments        int
	Events           int
	Seed             int64
	ExporterUser     string
	ExporterPassword string
	Retries          int
	RetryWait        time.Duration
}

const schemaSQL = `
DROP TABLE IF EXISTS shipment_events CASCADE;
DROP TABLE IF EXISTS shipments CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS clients CASCADE;

CREATE TABLE clients (
    client_id  BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL,
    phone      TEXT NOT NULL,
    country    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE drivers (
    driver_id  BIGINT PRIMARY KEY,
    full_name  TEXT NOT NULL,
    phone      TEXT NOT NULL,
    license_no TEXT NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE vehicles (
    vehicle_id  BIGINT PRIMARY KEY,
    plate_no    TEXT NOT NULL,
    model       TEXT NOT NULL,
    capacity_kg INTEGER NOT NULL,
    status      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE shipments (
    shipment_id      BIGINT PRIMARY KEY,
    client_id        BIGINT NOT NULL REFERENCES clients(client_id),
    driver_id        BIGINT NOT NULL REFERENCES drivers(driver_id),
    vehicle_id       BIGINT NOT NULL REFERENCES vehicles(vehicle_id),
    origin_city      TEXT NOT NULL,
    destination_city TEXT NOT NULL,
    distance_km      NUMERIC(8,2) NOT NULL,
    weight_kg        NUMERIC(10,2) NOT NULL,
    price            NUMERIC(12,2) NOT NULL,
    status           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ
);

CREATE TABLE shipment_events (
    event_id   BIGINT PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES shipments(shipment_id),
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    location   TEXT NOT NULL,
    note       TEXT
);
`

var (
	cities      = []string{"Moscow", "Berlin", "Warsaw", "Amsterdam", "Paris", "Madrid", "Rome", "Vienna", "Prague", "Lisbon"}
	countries   = []string{"RU", "DE", "PL", "NL", "FR", "ES", "IT", "AT", "CZ", "PT"}
	shipStatus  = []string{"created", "in_transit", "delivered", "cancelled"}
	eventTypes  = []string{"picked_up", "in_transit", "customs", "out_for_delivery", "delivered", "exception"}
	vehicleMods = []string{"Volvo FH", "Scania R", "MAN TGX", "DAF XF", "Mercedes Actros"}
)

func Run(ctx context.Context, opts Options, logger *slog.Logger) error {
	conn, err := connect(ctx, opts, logger)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	logger.Info("creating schema")
	if _, err := conn.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	rng := rand.New(rand.NewSource(opts.Seed))
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := seedClients(ctx, conn, opts, rng, base, logger); err != nil {
		return err
	}
	if err := seedDrivers(ctx, conn, opts, rng, base, logger); err != nil {
		return err
	}
	if err := seedVehicles(ctx, conn, opts, rng, base, logger); err != nil {
		return err
	}
	if err := seedShipments(ctx, conn, opts, rng, base, logger); err != nil {
		return err
	}
	if err := seedEvents(ctx, conn, opts, rng, base, logger); err != nil {
		return err
	}
	if err := provisionReadOnly(ctx, conn, opts.ExporterUser, opts.ExporterPassword); err != nil {
		return err
	}

	logger.Info("seed finished",
		"clients", opts.Clients,
		"drivers", opts.Drivers,
		"vehicles", opts.Vehicles,
		"shipments", opts.Shipments,
		"shipment_events", opts.Events,
	)
	return nil
}

func connect(ctx context.Context, opts Options, logger *slog.Logger) (*pgx.Conn, error) {
	var lastErr error
	for attempt := 1; attempt <= opts.Retries; attempt++ {
		conn, err := pgx.Connect(ctx, opts.DB.DSN())
		if err == nil {
			return conn, nil
		}
		lastErr = err
		logger.Warn("database not ready, retrying", "attempt", attempt, "of", opts.Retries, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(opts.RetryWait):
		}
	}
	return nil, fmt.Errorf("connect to database after %d attempts: %w", opts.Retries, lastErr)
}

func seedClients(ctx context.Context, conn *pgx.Conn, opts Options, rng *rand.Rand, base time.Time, logger *slog.Logger) error {
	cols := []string{"client_id", "name", "email", "phone", "country", "created_at"}
	src := pgx.CopyFromSlice(opts.Clients, func(i int) ([]any, error) {
		id := int64(i + 1)
		return []any{
			id,
			fmt.Sprintf("Client %d GmbH", id),
			fmt.Sprintf("contact%d@client.example.com", id),
			fmt.Sprintf("+49-30-%07d", id),
			countries[rng.Intn(len(countries))],
			base.AddDate(0, 0, rng.Intn(365)),
		}, nil
	})
	return copyFrom(ctx, conn, "clients", cols, src, logger)
}

func seedDrivers(ctx context.Context, conn *pgx.Conn, opts Options, rng *rand.Rand, base time.Time, logger *slog.Logger) error {
	cols := []string{"driver_id", "full_name", "phone", "license_no", "status", "created_at"}
	src := pgx.CopyFromSlice(opts.Drivers, func(i int) ([]any, error) {
		id := int64(i + 1)
		status := "active"
		if rng.Intn(10) == 0 {
			status = "inactive"
		}
		return []any{
			id,
			fmt.Sprintf("Driver %d", id),
			fmt.Sprintf("+49-170-%07d", id),
			fmt.Sprintf("DL-%08d", id),
			status,
			base.AddDate(0, 0, rng.Intn(365)),
		}, nil
	})
	return copyFrom(ctx, conn, "drivers", cols, src, logger)
}

func seedVehicles(ctx context.Context, conn *pgx.Conn, opts Options, rng *rand.Rand, base time.Time, logger *slog.Logger) error {
	cols := []string{"vehicle_id", "plate_no", "model", "capacity_kg", "status", "created_at"}
	src := pgx.CopyFromSlice(opts.Vehicles, func(i int) ([]any, error) {
		id := int64(i + 1)
		return []any{
			id,
			fmt.Sprintf("B-XY-%04d", id),
			vehicleMods[rng.Intn(len(vehicleMods))],
			5000 + rng.Intn(20000),
			"available",
			base.AddDate(0, 0, rng.Intn(365)),
		}, nil
	})
	return copyFrom(ctx, conn, "vehicles", cols, src, logger)
}

func seedShipments(ctx context.Context, conn *pgx.Conn, opts Options, rng *rand.Rand, base time.Time, logger *slog.Logger) error {
	cols := []string{
		"shipment_id", "client_id", "driver_id", "vehicle_id",
		"origin_city", "destination_city", "distance_km", "weight_kg",
		"price", "status", "created_at", "delivered_at",
	}
	src := pgx.CopyFromSlice(opts.Shipments, func(i int) ([]any, error) {
		id := int64(i + 1)
		origin := cities[rng.Intn(len(cities))]
		dest := cities[rng.Intn(len(cities))]
		for dest == origin {
			dest = cities[rng.Intn(len(cities))]
		}
		status := shipStatus[rng.Intn(len(shipStatus))]
		created := base.Add(time.Duration(rng.Intn(365*24)) * time.Hour)

		var delivered any
		if status == "delivered" {
			delivered = created.Add(time.Duration(12+rng.Intn(120)) * time.Hour)
		}
		distance := 50 + rng.Float64()*2950
		weight := 100 + rng.Float64()*23900
		return []any{
			id,
			int64(rng.Intn(opts.Clients) + 1),
			int64(rng.Intn(opts.Drivers) + 1),
			int64(rng.Intn(opts.Vehicles) + 1),
			origin,
			dest,
			round2(distance),
			round2(weight),
			round2(distance*1.8 + weight*0.05),
			status,
			created,
			delivered,
		}, nil
	})
	return copyFrom(ctx, conn, "shipments", cols, src, logger)
}

func seedEvents(ctx context.Context, conn *pgx.Conn, opts Options, rng *rand.Rand, base time.Time, logger *slog.Logger) error {
	cols := []string{"event_id", "shipment_id", "event_type", "event_time", "location", "note"}
	src := pgx.CopyFromSlice(opts.Events, func(i int) ([]any, error) {
		id := int64(i + 1)
		return []any{
			id,
			int64(rng.Intn(opts.Shipments) + 1),
			eventTypes[rng.Intn(len(eventTypes))],
			base.Add(time.Duration(rng.Intn(365*24)) * time.Hour),
			cities[rng.Intn(len(cities))],
			"",
		}, nil
	})
	return copyFrom(ctx, conn, "shipment_events", cols, src, logger)
}

func copyFrom(ctx context.Context, conn *pgx.Conn, table string, cols []string, src pgx.CopyFromSource, logger *slog.Logger) error {
	started := time.Now()
	n, err := conn.CopyFrom(ctx, pgx.Identifier{table}, cols, src)
	if err != nil {
		return fmt.Errorf("copy into %s: %w", table, err)
	}
	logger.Info("table seeded", "table", table, "rows", n, "duration", time.Since(started).String())
	return nil
}

func provisionReadOnly(ctx context.Context, conn *pgx.Conn, user, password string) error {
	role := pgx.Identifier{user}.Sanitize()
	stmt := fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD %s", role, quoteLiteral(password))
	if _, err := conn.Exec(ctx, stmt); err != nil && !isDuplicateRole(err) {
		return fmt.Errorf("create role %s: %w", user, err)
	}
	grants := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", role),
		fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", role),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s", role),
	}
	for _, g := range grants {
		if _, err := conn.Exec(ctx, g); err != nil {
			return fmt.Errorf("grant privileges: %w", err)
		}
	}
	return nil
}

func isDuplicateRole(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42710" // duplicate_object
	}
	return false
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}
