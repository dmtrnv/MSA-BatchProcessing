FROM golang:1.24-alpine AS build

WORKDIR /src
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS=-trimpath

COPY exporter/go.mod exporter/go.sum ./
RUN go mod download

COPY exporter/ ./

RUN go build -ldflags="-s -w" -o /out/exporter ./cmd/exporter \
 && go build -ldflags="-s -w" -o /out/seed ./cmd/seed

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/exporter /app/exporter
COPY --from=build /out/seed /app/seed

USER nonroot:nonroot

ENTRYPOINT ["/app/exporter"]
